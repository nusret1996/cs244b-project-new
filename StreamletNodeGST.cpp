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
 * Implements the Streamlet protocol in which votes are allowed to arrive after the epoch in
 * which a block is proposed. However, epochs still track physical time, and in particular,
 * a new block cannot be proposed until the duration of physical time corresponding to an
 * epoch has elapsed, even if the block proposed in a prior epoch has been notarized.
 *
 * The service implements the RPC server using the synchronous interface and acts as an RPC
 * client using the asynchronous interface so that broadcasts to other nodes do not block
 * request processing.
 */
class StreamletNodeGST : public Streamlet::Service {
public:
    /*
     * Params
     *   id: Index in peers that is the ID of this node
     *   peers: Vector of peers in the including the local node
     *   priv: Private key of local node
     *   pub: Public key of local node
     *   epoch_len: Duration of the epoch in milliseconds
     */
    StreamletNodeGST(
        uint32_t id,
        const std::vector<Peer> &peers,
        const Key &priv,
        const std::chrono::milliseconds &epoch_len,
        ReplicatedStateMachine *rsm_client
    );

    ~StreamletNodeGST();

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

    void record_proposal(const Proposal *proposal, uint64_t epoch);

    /*
     * Links block proposed in note_epoch to the notarized chain under parent proposed in par_epoch.
     * Called from ProposeBlock once the block contents are known. This should only be called once
     * for a given note_epoch once the block for that epoch gains enough votes.
     *
     * Notarization of a block is recorded by storing a pointer to a ChainElement that has its
     * block fields filled into successors. If a block's parent has not been notarized, then a
     * placeholder ChainElement is constructed with only the parent's epoch number and a pointer
     * to it is stored into successors. This allows later notarizations to be recorded and later
     * relinked as the predecessors get notarized.
     *
     * If the chain index of the notarized block is known, which is the case if its parent has already
     * been notarized and the same holds inductively stretching back to the genesis block, a BFS is
     * run from the notarized block over its successors to update their chain indexes. Any prior notarized
     * blocks that have formed a "floating" part of the chain have their chain indexes updated in this way
     * and this is also how max_chainlen is tracked and updated. This BFS procedure maintains the invariant
     * that a block's chain position is known if and only if its parent's is known.
     * 
     * Finalization is also detected during the BFS. If a finalization is detected during BFS, a walk
     * backwards up the chain is made. If last_finalized can be reached, then it is known that the
     * finalized chain is not missing any notarizations, and any branches extending from blocks in the
     * finalized chain that are not part of the finalized chain are removed (via repeated BFS as the
     * walk progresses up the chain). ChainElements on these branches are garbage collected, and future
     * RPCs will know to discard messages for that block (including additional votes, which are no
     * longer needed). The distributed application's (client's) callback is then invoked for each newly
     * finalized block after last_finalized, which is also updated to the latest finalized block.
     * Otherwise, there is at least one block on the finalized chain whose notarization has not yet been
     * seen, and last_finalized remains unchanged.
     */
    void notarize_block(
        const Block &note_block,
        const std::string &note_hash,
        uint64_t note_epoch,
        uint64_t par_epoch
    );

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

    // last element of longest notarized chain (not necessarily unique),
    // also guarded by successors_m
    const ChainElement *last_chainlen;

    // Pointer to one past last finalized ChainElement in successors,
    // which is the block at which to start notifying the client app
    // of new finalizations, also guarded by successors_m
    const ChainElement *last_finalized;

    // Mutex for successors
    std::mutex successors_m;

    // Mutex to serialize client notifications, and also to
    // ensure finalization notifications occur in order
    std::mutex notification_m;

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

StreamletNodeGST::StreamletNodeGST(
    uint32_t id,
    const std::vector<Peer> &peers,
    const Key &priv,
    const std::chrono::milliseconds &epoch_len,
    ReplicatedStateMachine *rsm_client)
    : network{peers, id},
    crypto{peers, priv, peers.at(id).key},
    client{rsm_client},
    max_chainlen{0},
    genesis_block{0},
    local_addr{peers.at(id).addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    note_threshold{((num_peers * 2) / 3) + (num_peers % 3)},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch_counter{0}
{
    // Create entry for dependents of genesis block
    successors.emplace(0, &genesis_block);

    // Set the hash of the empty block
    crypto.hash_block(&genesis_block.block, genesis_block.hash);

    // Set pointer once genesis block is constructed
    last_finalized = &genesis_block;
    last_chainlen = &genesis_block;
}

StreamletNodeGST::~StreamletNodeGST() {

}

grpc::Status StreamletNodeGST::NotifyVote(
    grpc::ServerContext* context,
    const Vote* vote,
    Response* response
) {
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    const uint64_t b_epoch = vote->epoch();
    const std::string& b_hash = vote->hash();
    const uint32_t voter = vote->node();

    if (!crypto.verify_signature(voter, b_hash, vote->signature())) {
        std::cout << "Warning: Signature verification failed on vote for block " << b_epoch
            << " by node " << voter <<  ", discarding message" << std::endl;
        return grpc::Status::OK; // discard message
    }

    // Cleanup the block
    bool remove = false;

    // The number of votes including the vote from the message being
    // processed if it is valid
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

        if (remove) {
            delete cand;
        } else if (votes > 0) {
            // Votes can only be non-zero if the proposal has already been seen. Furthermore, if this
            // invocation of NotifyVote has incremented the number of votes to meet the notarization
            // threshold, then the block must be notarized by the same invocation.
            if (votes == note_threshold) {
                // can lock candidates map, remove from map, add entry to some "finished" set, and then unlock
                // then call notarize_block() with the Candidate's block
                // then lock the Candidate, set the remove flag, record ref_count.load(std::memory_order_relaxed),
                // and then unlock, and finally deallocate the candidate if ref_count was zero

                // but for now just notarize
                notarize_block(cand->block, b_hash, b_epoch, vote->parent());
            }

            // votes is 0 when the message is a duplicate or unanticipated, echo otherwise
            network.broadcast(*vote, &req_queue);
        }

        // As future work, NotifyVote could also notarize blocks whose proposal has not been seen if a
        // mechanism to catch up by recursively requesting missing proposals and parents is implemented.
    }

    return grpc::Status::OK;
}

grpc::Status StreamletNodeGST::ProposeBlock(
    grpc::ServerContext* context,
    const Proposal* proposal,
    Response* response
) {
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    const uint64_t b_epoch = proposal->block().epoch();
    const uint64_t b_parent = proposal->block().parent();
    const uint32_t proposer = proposal->node();

    if (proposer != crypto.hash_epoch(b_epoch)) {
        std::cout << "Warning: Received proposal for epoch " << b_epoch
            << " from wrong leader (node " << proposer
            << "), discarding message" << std::endl;
        return grpc::Status::OK; // discard mesasge
    }

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);
    if (!crypto.verify_signature(proposer, hash, proposal->signature())) {
        std::cout << "Warning: Signature verification failed for block " << b_epoch
            << " proposed by node " << proposer <<  ", discarding message" << std::endl;
        return grpc::Status::OK; // discard mesasge
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
            } else if (iter->second->hash == proposal->block().phash()) {
                index_known = true;
            } // else the parent hash did not match so do not vote
        } else if (iter != successors.end() && b_parent == 0) {
            if (max_chainlen > 0) {
                outdated_length = true;
            } else if (iter->second->hash == proposal->block().phash()) {
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
        std::cout << "Proposal for epoch " <<  b_epoch
            << " does not extend longest chain, discarding message" << std::endl;
        return grpc::Status::OK; // discard message
    }

    // Check that the block is a valid extension according to the application
    // by invoking a callback on the ReplicatedStateMachine.
    if (!client->ValidateTransactions(proposal->block().txns(), proposal->block().epoch())) {
        std::cout << "Warning: Client rejected proposal for epoch " << b_epoch
            << ", discarding message" << std::endl;
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
            notarize_block(proposal->block(), hash, b_epoch, b_parent);
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

void StreamletNodeGST::notarize_block(
    const Block &note_block,
    const std::string &note_hash,
    uint64_t note_epoch,
    uint64_t par_epoch
) {
    notification_m.lock();
    client->TransactionsNotarized(note_block.txns(), note_epoch);
    notification_m.unlock();

    std::list<ChainElement*> queue;
    const ChainElement *prev_finalized = nullptr;
    ChainElement *new_finalized = nullptr;

    if (note_epoch == 0) {
        std::cerr << "Genesis block should never be passed to notarize_block" << std::endl;
        return;
    }

    successors_m.lock();

    // In the time that ProposeBlock was running, it is possible that a block on a different branch
    // has extended the finalized chain, in which case a ChainElement for note_epoch must not be
    // constructed because the ChainElement at par_epoch has been deleted and the new ChainElement
    // would not later be garbage collected.
    if (par_epoch < last_finalized->block.epoch()) {
        successors_m.unlock();
        return;
    }

    ChainElement *&p_elem = successors[par_epoch];
    if (p_elem == nullptr) {
        p_elem = new ChainElement{par_epoch};
    }

    ChainElement *&new_elem = successors[note_epoch];
    if (new_elem == nullptr) {
        new_elem = new ChainElement{note_epoch};
        new_elem->block.CopyFrom(note_block);
        new_elem->hash = note_hash;
    } else if (new_elem->index == 0) {
        // ChainElement already existed but block was previously not notarized
        new_elem->block.CopyFrom(note_block);
        new_elem->hash = note_hash;
    } else {
        successors_m.unlock();
        std::cerr << "Duplicate attempt to notarize block " << note_epoch << std::endl;
        return;
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

        // Because queue is expanded in order of increasing BFS depth, the
        // last element must contain the length of the longest notarized
        // chain, although this chain is not necessarily unique
        last_chainlen = queue.back();
        max_chainlen = last_chainlen->index;
    }

    if (finalize_end != queue.end()) {
        // The last finalized element is second to last element in the
        // contiguous sequence of epochs. This will be used to update
        // last_finalized below if the tail of the finalized chain can
        // be walked all the way back to last_finalized.
        new_finalized = (*finalize_end)->plink;

        queue.clear();

        std::list<ChainElement*>::iterator queue_iter = queue.end();

        // In this branch, since finalize_end was set, its plink is known
        // not to be null as is its grandparent (*finalize_end)->plink->plink,
        // so that the first iteration of the loop below is safe
        ChainElement *elem = new_finalized;

        while (elem != last_finalized && elem->index != 0) {
            ChainElement *p = elem->plink;
            std::list<ChainElement*>::iterator pchild = p->links.begin();
            std::list<ChainElement*>::iterator rm;

            // This loop pushes siblings of elem, which are now known not to be
            // in the finalized chain, into queue to be deleted below
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

            last_finalized = new_finalized;

            // Remove outdated chains from the successors map
            for (ChainElement *rm : queue) {
                successors.erase(rm->epoch);
            }
        }
    }

    // Overlap locks because, in case of concurrent finalizations, the thread that
    // will notify the client of earlier finalized nodes must run first
    notification_m.lock();
    successors_m.unlock();

    std::list<const ChainElement*> finalized_elems;

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
            std::cerr << "Block " <<  prev_finalized->epoch
                << " on finalized chain does not have exactly one successor" << std::endl;
        }

        do {
            prev_finalized = prev_finalized->links.front();

            client->TransactionsFinalized(prev_finalized->block.txns(), prev_finalized->block.epoch());

        } while (prev_finalized != new_finalized);
    }

    notification_m.unlock();
}

void StreamletNodeGST::broadcast_vote(const Proposal* proposal, const std::string &hash) {
    Vote v;
    v.set_node(local_id);
    v.set_parent(proposal->block().parent());
    v.set_epoch(proposal->block().epoch());
    v.set_hash(hash);

    crypto.sign_sha256(hash, v.mutable_signature());

    network.broadcast(v, &req_queue);
}

void StreamletNodeGST::record_proposal(const Proposal *proposal, uint64_t epoch) {
    bool remove = false;
    uint32_t votes = 0;
    Candidate *cand = nullptr;

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);

    candidates_m.lock();
    std::unordered_map<uint64_t, Candidate*>::iterator iter
        = candidates.find(epoch);

    if (iter == candidates.end()) {
        cand = new Candidate{num_peers};
        candidates.emplace(epoch, cand);
    } else {
        cand = iter->second;
    }

    cand->ref_count.fetch_add(1, std::memory_order_relaxed);
    candidates_m.unlock();

    // Unlike in ProposeBlock, the check on hash.empty() is to prevent double counting
    // in case an echo from a remote node has returned the local proposal before this
    // method was run.
    cand->m.lock();
    if (cand->hash.empty()) {
        cand->hash = hash;

        // Since the local node proposed the block, only count one vote
        votes = 1;
        cand->voters[local_id] = true;

        // Tally other votes in case votes from other nodes started rolling in
        // before local execution reached this point
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
        notarize_block(proposal->block(), hash, epoch, proposal->block().parent());
    }
}

void StreamletNodeGST::Run(system_clock::time_point epoch_sync) {
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

            std::cout << "Timeout delay: " << (t_now - epoch_sync).count() << std::endl;

            // Since the increment specifies relaxed memory order, be careful
            // that no code below this statement changes other data shared among
            // threads that must be sequenced with epoch_counter.
            uint64_t cur_epoch = epoch_counter.fetch_add(1, std::memory_order_relaxed);

            cur_epoch++; // Must be incremented because the atomic op returns the previous value

            if (crypto.hash_epoch(cur_epoch) == local_id) {
                // Run leader logic
                Proposal p;

                p.set_node(local_id);
                // todo: add signature when crypto library done
                // p.set_signature();
                p.mutable_block()->set_parent(max_chainlen);

                successors_m.lock();
                p.mutable_block()->set_phash(last_chainlen->hash);
                successors_m.unlock();

                p.mutable_block()->set_epoch(cur_epoch);

                client->GetTransactions(p.mutable_block()->mutable_txns(), cur_epoch);
                std::cout << "Epoch " << cur_epoch << ", leader " << local_id
                    << ": " << p.block().txns() << std::endl;

                network.broadcast(p, &req_queue);

                // Broadcast first then propose locally to overlap the network requests
                // with local exection. record_proposal is designed to be safe to call
                // with the possiblity of from remote nodes echoing the proposal so that
                // ProposeBlock is called before or concurrently with record_proposal.
                record_proposal(&p, cur_epoch);
            }
        }

        // Nothing to process if the status is TIMEOUT. Otherwise, tag is a
        // completed async request that must be cleaned up.
        if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            NetworkInterposer::Pending *req = static_cast<NetworkInterposer::Pending*>(tag);

            // optionally check status and response (currently an empty struct)

            // Deallocate the Pending structure
            delete req;
        }

        status = req_queue.AsyncNext(&tag, &ok, epoch_sync);
    }

    server->Shutdown();
    server->Wait();
}

int main(const int argc, const char *argv[]) {
    if (argc != 5) {
        std::cout << "Usage: StreamletNodeGST <sync_time> <epoch_len> <config_file> <local_id>\n"
            << "where\n"
            << "\tsync_time is of the form HH:MM:SS specifying at time in UTC at which to start counting epochs\n"
            << "\tepoch_len is the duration of each epoch in milliseconds\n"
            << "\tconfig_file is a path to a copy of the configuration file supplied to all nodes in the system\n"
            << "\tlocal_id is the node ID used by the instance being started\n"
            << std::endl;
        return 1;
    }
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

    StreamletNodeGST service{
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