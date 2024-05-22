#include "grpc/grpc.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"

#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/completion_queue.h"

#include "streamlet.grpc.pb.h"

#include <iostream>
#include <mutex>

#include "structs.h"
#include "utils.h"
#include "NetworkInterposer.h"
#include "CryptoManager.h"
#include "KeyValueStateMachine.h"

using std::chrono::system_clock;

/*
 * Implements the Streamlet protocol in which votes are allowed to arrive after
 * the epoch in which a block is proposed. However, epochs still track physical
 * time, and in particular, a new block cannot be proposed until the duration
 * of physical time corresponding to an epoch has elapsed, even if the block
 * proposed in a prior epoch has been notarized.
 *
 * The service implements the RPC server using the synchronous interface
 * and acts as an RPC client using the asynchronous interface so that
 * broadcasts to other nodes do not block request processing.
 */
class StreamletNodeV1 : public Streamlet::Service {
public:
    /*
     * Params
     *   id: Index in peers that is the ID of this node
     *   peers: Vector of peers in the including the local node
     *   priv: Private key of local node
     *   pub: Public key of local node
     *   epoch_len: Duration of the epoch in milliseconds
     */
    StreamletNodeV1(
        uint32_t id,
        const std::vector<Peer> &peers,
        const Key &priv,
        const std::chrono::milliseconds &epoch_len,
        ReplicatedStateMachine *rsm_client
    );

    ~StreamletNodeV1();

    grpc::Status NotifyVote(
        grpc::ServerContext* context,
        const Vote* vote,
        Response* response
    ) override;

    grpc::Status ProposeBlock(
        grpc::ServerContext* context,
        const Proposal* proposal,
        Response* response
    ) override;

    /*
     * Processes finished RPCs and epoch advancement, intended to take over the main
     * thread. Finished RPCs only need to be cleaned up since, for now, the server side
     * of the Streamlet service is not expected to return any information. A deadline
     * is set at the start of the next epoch wait on the completion queue so
     * that it does not miss the start of an epoch if the queue is empty. Regardless,
     * advancement of the epoch is always checked first whenever anything is dequeued.
     *
     * Params
     *   epoch_sync: Start of the initial epoch in local time. Must be the same, relative
     *               to the local time zone, as the epoch synchronization point of other
     *               nodes in the system.
     */
    void Run(system_clock::time_point epoch_sync);

    /*
     * Queues transactions, which are a binary, to be placed into a block and proposed
     * at some future time when this node becomes leader.
     */
    void QueueTransactions();

private:
    void broadcast_vote(const Proposal* proposal, const std::string &hash);

    void notarize_block(const Block &note_block, uint32_t note_epoch, uint32_t par_epoch);

    NetworkInterposer network;

    CryptoManager crypto;

    ReplicatedStateMachine* client;

    // Completion queue for outbound RPCs
    grpc::CompletionQueue req_queue;

    // Blocks that have been seen or proposed and that are awaiting votes
    std::unordered_map<uint64_t, Candidate*> candidates;

    // Mutex for candidates. Short reads in common case so better not
    // having reader/writer lock.
    std::mutex candidates_m;

    // Mapping from a block to its dependents so that the chain can
    // be constructed with later blocks that were notarized first and
    // were waiting on a given earlier block
    std::unordered_map<uint64_t, ChainElement*> successors;

    // Length of longest notarized chain, also guarded by successors_m
    uint64_t max_chainlen;
    // last element of longest notarized chain (not necessarily unique), also guarded by successors_m
    const ChainElement *last_chainlen;

    // Pointer to one past last finalized ChainElement in successors,
    // which is the block at which to start notifying the client app
    // of new finalizations, also guarded by successors_m
    const ChainElement *last_finalized;

    // Mutex for successors
    std::mutex successors_m;

    // Genesis block
    ChainElement genesis_block;

    // Address of this server
    const std::string local_addr;

    // The ID of this node
    const uint32_t local_id;

    // Number of peers, which is one greater than the max ID
    const uint32_t num_peers;

    // Number of votes at which a block is notarized
    const uint32_t note_threshold;

    // Duration of each epoch
    const system_clock::duration epoch_duration;

    // Current epoch number
    std::atomic_uint64_t epoch_counter;
};

StreamletNodeV1::StreamletNodeV1(
    uint32_t id,
    const std::vector<Peer> &peers,
    const Key &priv,
    const std::chrono::milliseconds &epoch_len,
    ReplicatedStateMachine *rsm_client)
    : network{peers},
    crypto{peers, priv, peers.at(id).key},
    client{rsm_client},
    max_chainlen{0},
    genesis_block{0},
    local_addr{peers.at(id).addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    note_threshold{((num_peers + 2) / 3) * 2},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch_counter{0}
     {

    // Create entry for dependents of genesis block
    successors.emplace(0, &genesis_block);

    // Set pointer once genesis block is constructed
    last_finalized = &genesis_block;
    last_chainlen = &genesis_block;
}

StreamletNodeV1::~StreamletNodeV1() {

}

grpc::Status StreamletNodeV1::NotifyVote(
    grpc::ServerContext* context,
    const Vote* vote,
    Response* response
) {
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    const uint64_t b_epoch = vote->epoch();
    const std::string& b_hash = vote->hash();
    const uint32_t voter = vote->node();

    if (!crypto.verify_signature(voter, b_hash, vote->signature())) {
        return grpc::Status::OK; // discard message
    }

    // Cleanup the block
    bool remove = false;

    // The number of votes including the vote from the message being
    // processed, if it is valid
    uint32_t votes = 0;

    // Pointer to candidate set only when the vote is in scope for the epoch
    Candidate *cand = nullptr;

    // discard and ignore or report error if b_epoch > cur_epoch
    if (b_epoch <= cur_epoch) {
        candidates_m.lock();
        std::unordered_map<uint64_t, Candidate*>::iterator iter
            = candidates.find(b_epoch);

        if (iter == candidates.end()) {
            cand = new Candidate{num_peers};
            candidates.emplace(b_epoch, cand);
        } else {
            cand = iter->second;
        }

        cand->ref_count.fetch_add(1, std::memory_order_relaxed);
        candidates_m.unlock();
    }

    if (cand != nullptr) {
        cand->m.lock();
        // Empty hash means the proposal has not yet been seen
        if (cand->hash.empty()) {
            cand->vote_hashes.emplace(voter, b_hash);
        } else if (cand->hash == b_hash && cand->voters[voter] != true) {
            cand->voters[voter] = true;
            votes = ++cand->votes;
        }
        remove = (cand->removed
            && (cand->ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        cand->m.unlock();

        // Apply notarization procedure if and only if remove == false and votes == 2/3 majority.
        // This ensures the notarization procedure is applied only once, namely by the thread
        // that records the notarizing vote. Duplicate messages do not change votes from its zero
        // initialization and so will not indicate notarization a second time.
        if (remove) {
            delete cand;
        } else if (votes > 0) {
            // votes is 0 when the message is a duplicate or unanticipated, echo otherwise
            network.broadcast(*vote, &req_queue);
        }
    }

    return grpc::Status::OK;
}

grpc::Status StreamletNodeV1::ProposeBlock(
    grpc::ServerContext* context,
    const Proposal* proposal,
    Response* response
) {
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    const uint64_t b_epoch = proposal->block().epoch();
    const uint64_t b_parent = proposal->block().parent();
    const uint32_t proposer = proposal->node();

    if (proposer != crypto.hash_epoch(cur_epoch)) {
        std::cout << "from previous epoch, discard" << std::endl;
        return grpc::Status::OK; // discard message
    }

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);
    if (!crypto.verify_signature(proposer, hash, proposal->signature())) {
        std::cout << "crypto verify signature failed, discard" << std::endl;
        return grpc::Status::OK; // discard message
    }

    // Check that the block extends the longest chain. Since messages can be delayed
    // and the proposal might not have been seen by the time a block is notarized,
    // it is not always possible to check this. A proposal can be definitely ruled out
    // if its parent's index is known and is less than the maximum notarized chain length.
    // Since a ChainElement's index is 0 when its position is not known, and the genesis
    // block's index is also 0, this definite condition is checked in two cases, one for
    // when the parent block is the genesis block and one for when it is not. The one
    // other definite condition is when a proposed block's parent has an epoch number
    // earlier than the most recent finalized block. This also ensures that when blocks
    // not in the finalized chain are deleted in notarize_block() below, no later messages
    // will allocate a new ChainElement for that parent.
    bool outdated_length = false;
    bool index_known = false;
    successors_m.lock();
    if (b_parent < last_finalized->block.epoch()) {
        outdated_length = true;
    } else {
        std::unordered_map<uint64_t, ChainElement*>::iterator iter
            = successors.find(b_parent);

        if (iter != successors.end() && b_parent != 0 && iter->second->index != 0) {
            if (iter->second->index < max_chainlen) {
                outdated_length = true;
            } else {
                index_known = true;
            }
        } else if (iter != successors.end() && b_parent == 0) {
            if (max_chainlen > 0) {
                outdated_length = true;
            } else {
                index_known = true;
            }
        }

        // if (iter != successors.end() &&
        //     ((b_parent != 0 && iter->second.index != 0 && iter->second.index < max_chainlen)
        //     || (b_parent == 0 && max_chainlen > 0))) {
        //         outdated_length = true;
        // }
    }
    successors_m.unlock();

    if (outdated_length) {
        std::cout << "does not extend longest chain, discard" << std::endl;
        return grpc::Status::OK; // discard message
    }

    // Check that the block is a valid extension according to the application
    // by invoking a callback on the ReplicatedStateMachine.
    if (!client->ValidateTransaction(proposal->block().txns())) {
        std::cout << "RSM validate transaction failed, discard" << std::endl;
        return grpc::Status::OK; // discard message
    }

    bool remove = false;
    uint32_t votes = 0;
    Candidate *cand = nullptr;

    // discard and ignore or report error if b_epoch > cur_epoch
    if (b_epoch <= cur_epoch) {
        candidates_m.lock();
        std::unordered_map<uint64_t, Candidate*>::iterator iter
            = candidates.find(b_epoch);

        if (iter == candidates.end()) {
            cand = new Candidate{num_peers};
            candidates.emplace(b_epoch, cand);
        } else {
            cand = iter->second;
        }

        cand->ref_count.fetch_add(1, std::memory_order_relaxed);
        candidates_m.unlock();
    }

    if (cand != nullptr) {
        // An empty hash means the proposal has not yet been seen. If it has been
        // seen, then this is a duplicate or invalid message and must be ignored.
        cand->m.lock();
        if (cand->hash.empty()) {
            cand->hash = hash;

            // Proposer's vote is always recorded below. Add the local vote if the epoch
            // matches and the chain length has been verified. Otherwise, only count
            // the proposers vote,
            if (b_epoch == cur_epoch && index_known) {
                votes = 2;
                cand->voters[local_id] = true;
            } else {
                votes = 1;
            }

            cand->voters[proposer] = true;

            for (std::pair<const uint32_t, std::string> &p : cand->vote_hashes) {
                if (p.second == hash) {
                    cand->voters[p.first] = true;
                    votes++;
                }
            }
            cand->votes = votes;
            cand->vote_hashes.clear();

            cand->block.CopyFrom(proposal->block());
        }
        remove = (cand->removed
            && (cand->ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        cand->m.unlock();

        // See comment above identical branch in NotifyVote. Notarization will not
        // be applied a second time if the message is a duplicate since votes is 0.
        if (remove) {
            delete cand;
        } else if (votes >= note_threshold) {
            // can lock candidates map, remove from map, add entry to some "finished" set, and then unlock
            // then call notarize_block() with the Candidate's block
            // then lock the Candidate, set the remove flag, record ref_count.load(std::memory_order_relaxed),
            // and then unlock, and finally deallocate the candidate if ref_count was zero

            // but for now just notarize
            notarize_block(proposal->block(), b_epoch, b_parent);
        }

        // votes is 0 when the message is a duplicate or unanticipated, echo otherwise
        if (!remove && votes > 0) {
            network.broadcast(*proposal, &req_queue);

            if (b_epoch == cur_epoch && index_known) {
                broadcast_vote(proposal, hash);
            }
        }
    }

    return grpc::Status::OK;
}

/*
 * notarize_block must be called only once for a given block/epoch id.
 */
void StreamletNodeV1::notarize_block(const Block &note_block, uint32_t note_epoch, uint32_t par_epoch) {
    client->TransactionsNotarized(note_block.txns());

    std::list<ChainElement*> queue;
    const ChainElement *prev_finalized = nullptr;
    const ChainElement *new_finalized = nullptr;

    successors_m.lock();

    ChainElement *&p_elem = successors[par_epoch];
    if (p_elem == nullptr) {
        p_elem = new ChainElement{par_epoch};
    }

    ChainElement *&new_elem = successors[note_epoch];
    if (new_elem == nullptr) {
        new_elem = new ChainElement{note_epoch};
        new_elem->block.CopyFrom(note_block);
    } else if (!new_elem->block.IsInitialized()) {
        // ChainElement already existed but block was previously not notarized
        new_elem->block.CopyFrom(note_block);
    }

    new_elem->plink = p_elem;

    p_elem->links.push_back(new_elem);

    // If a finalization is detected, this is set to reference the last element
    // of a sequence of three or more contiguous epochs
    std::list<ChainElement*>::iterator finalize_end = queue.end();

    // If the parent is already linked into the chain, or the parent is the genesis
    // block, then set the chain index of new_elem and look for any children
    // that can be linked and check if they extend the longest chain at their position.
    if (p_elem->index != 0 || par_epoch == 0) {
        new_elem->index = p_elem->index + 1;

        queue.push_back(new_elem);

        // Check whether new_elem itself ends a finalized chain
        if (new_elem->epoch == p_elem->epoch + 1
            && (p_elem->plink != nullptr && p_elem->epoch == p_elem->plink->epoch + 1)) {
            finalize_end = queue.begin();
        }

        // Run a BFS to relink previously notarized blocks and detect finalization
        std::list<ChainElement*>::iterator queue_iter = queue.begin();
        while (queue_iter != queue.end()) {
            const ChainElement *elem = *queue_iter;
            bool parent_contiguous = elem->epoch == elem->plink->epoch + 1;

            for (ChainElement *child : elem->links) {
                child->index = elem->index + 1;

                queue.push_back(child);

                // If the child's epoch is contiguous with its parent's, and the parent
                // was contiguous with the grandparent, then the child is now the first
                // non-finalized element.
                if (parent_contiguous && child->epoch == elem->epoch + 1) {
                    finalize_end = --queue.end();
                }
            }

            ++queue_iter;
        }
    }

    queue.clear();

    std::cout << "before if statement" << std::endl;
    if (finalize_end != queue.end()) {
        std::cout << "check for finalize" << std::endl;
        // Because queue is expanded in order of increasing BFS depth, the
        // last element must contain the length of the longest notarized
        // chain, although this chain is not necessarily unique

        max_chainlen = queue.back()->index;
        last_chainlen = queue.back();
        std::list<ChainElement*>::iterator queue_iter = queue.end();

        // In this branch, since finalize_end was set, its plink is known
        // not to be null as is its grandparent (plink->plink), so that
        // the first iteration of the loop below is safe
        ChainElement *elem = (*finalize_end)->plink;

        while (elem != last_finalized && elem->index != 0) {
            ChainElement *p = elem->plink;
            std::list<ChainElement*>::iterator pchild = p->links.begin();
            std::list<ChainElement*>::iterator rm;
            while (pchild != p->links.end()) {
                if (*pchild != elem) {
                    queue.push_back(*pchild);
                    rm = pchild;
                    ++pchild;
                    p->links.erase(rm);
                } else {
                    ++pchild;
                }
            }

            // queue_iter will still be queue.end() on next iteration
            // if nothing was added to queue
            if (queue_iter == queue.end()) {
                queue_iter = queue.begin();
            } else {
                ++queue_iter;
            }

            // Run a BFS over the added elements to collect all elements
            // that are known not to be in the finalized chain
            while (queue_iter != queue.end()) {
                for (ChainElement *child : (*queue_iter)->links) {
                    queue.push_back(child);
                }
                ++queue_iter;
            }

            // Move iterator back to last element in queue so that it can
            // continue from the given position to newly added elements
            // in the next iteration of the outer while loop
            --queue_iter;

            elem = p;
        }

        if (elem == last_finalized) {
            prev_finalized = last_finalized;

            // The last finalized element is second to last element
            // in the contiguous sequence of epochs
            last_finalized = (*finalize_end)->plink;

            new_finalized = last_finalized;

            // Remove outdated chains from the successors map
            for (ChainElement *rm : queue) {
                successors.erase(rm->epoch);
            }
        }
    }

    successors_m.unlock();

    if (prev_finalized != nullptr) {
        // Deletion can be done outside the lock since elements are deleted
        // only when last_finalized has been updated, so no later blocks
        // can be notarized on any chain not extending from the new last
        // finalized block.
        for (ChainElement *elem : queue) {
            delete elem;
        }

        // Notify state machine client of finalized transactions. Similar to
        // the above, no new messages will change the state of these elements.
        // Moreover, notification is read-only, so notification does not require
        // taking hold of the lock. There also should be only one child per
        // element in the finalized chain, and the notifications begin from the
        // child of the previous last finalized element.
        if (prev_finalized->links.size() != 1) {
            std::cerr << "Finalized chain does not consist of single children" << std::endl;
        }

        do {
            prev_finalized = prev_finalized->links.front();

            client->TransactionsFinalized(prev_finalized->block.txns());

        } while (prev_finalized != new_finalized);
    }
}

void StreamletNodeV1::Run(system_clock::time_point epoch_sync) {
    // Build server and run
    std::string server_address{ local_addr };

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    // Poll for completed async requests and monitor epoch progress
    grpc::CompletionQueue::NextStatus status;
    void *tag;
    bool ok;
    status = req_queue.AsyncNext(&tag, &ok, epoch_sync);
    while (status != grpc::CompletionQueue::NextStatus::SHUTDOWN) {
        // Always check if the epoch advanced
        system_clock::time_point t_now = system_clock::now();

        if ((t_now - epoch_sync) >= epoch_duration) {
            epoch_sync += epoch_duration;
            // Since the increment specifies relaxed memory order, be careful
            // that no code below this statement changes other data shared among
            // threads that must be sequenced with epoch_counter.
            epoch_counter.fetch_add(1, std::memory_order_relaxed);

            if (crypto.hash_epoch(epoch_counter.load()) == local_id) {
                // run leader logic
                // propose block
                Proposal p;
                std::string hash;
                crypto.hash_block(&last_chainlen->block, hash);
                p.set_node(local_id);
                // tood: add signature when crypto library done
                // p.set_signature();
                p.mutable_block()->set_parent(max_chainlen);
                p.mutable_block()->set_phash(hash);
                p.mutable_block()->set_epoch(epoch_counter.load());
                p.mutable_block()->set_txns(client->GetTransactions());
                network.broadcast(p, &req_queue);
            }
        }

        // Nothing to process if the status is TIMEOUT. Otherwise, tag
        // is a completed async request that must be cleaned up.
        if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            NetworkInterposer::Pending *req = static_cast<NetworkInterposer::Pending*>(tag);

            // optionally check status and response (currently an empty struct)

            // cleanup code here
            delete req;
        }

        status = req_queue.AsyncNext(&tag, &ok, epoch_sync);
    }

    server->Shutdown();
    server->Wait();
}

void StreamletNodeV1::broadcast_vote(const Proposal* proposal, const std::string &hash) {
    Vote v;
    v.set_node(local_id);
    v.set_parent(proposal->block().parent());
    v.set_epoch(proposal->block().epoch());
    v.set_hash(hash);

    crypto.sign_sha256(hash, v.mutable_signature());

    if (!v.IsInitialized()) {
        std::cerr << "Missing fields on vote" << std::endl;
    }

    network.broadcast(v, &req_queue);
}

int main(const int argc, const char *argv[]) {
    // read command line: sync_time epoch_len config_file local_id
    // parse config into std::list<Peer>
    int status = 0;
    const uint32_t id = strtoul(argv[4], nullptr, 10);
    const uint32_t epoch = strtoul(argv[2], nullptr, 10);
    std::vector<Peer> peers;
    Key privkey;

    status = load_config(argv[3], id, peers, privkey);

    if (status == 1) {
        std::cerr << "Unable to open or read " << argv[3] << std::endl;
        return 1;
    } else if (status == 2) {
        std::cerr << "Configuration in " << argv[3] << " is malformed" << std::endl;
        return 1;
    }
    ReplicatedStateMachine* rsm = new KeyValueStateMachine(id);
    StreamletNodeV1 service{
        id,
        peers,
        privkey,
        std::chrono::milliseconds{epoch},
        rsm
    };

    // Run this as close to service.Run() as possible
    system_clock::time_point sync_start;
    status = sync_time(argv[1], sync_start);

    if (status == 1) {
        std::cerr << "Start time must be a valid HH:MM:SS" << std::endl;
        return 1;
    } else if (status == 2) {
        std::cerr << "Start time already passed" << std::endl;
        return 1;
    }

    std::cout << "Running as node " << id << " at " << peers[id].addr << std::endl;

    service.Run(sync_start);

    return 0;
}