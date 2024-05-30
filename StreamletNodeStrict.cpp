#include "grpcpp/grpcpp.h"

#include "streamlet.grpc.pb.h"

#include <iostream>
#include <mutex>

#include "structs.h"
#include "utils.h"
#include "NetworkInterposer.h"
#include "CryptoManager.h"
#include "ThroughputLossStateMachine.h"
#include "KeyValueStateMachine.h"

/*
 * Implements a stricter version Streamlet protocol that follows the protocol description
 * in the paper more closely. All messages concerning a block, including its proposal and
 * all votes, must arrive within an epoch. This implementation is therefore more susceptible
 * to clock skew, based on what external reference nodes have had their system clock synchronized
 * to, and clock drift. Within an epoch, however, proposals and votes may still arrive out
 * of order, unlike in the paper, since this cannot be controlled in a real network.
 *
 * The service implements the RPC server using the synchronous interface and acts as an RPC
 * client using the asynchronous interface so that broadcasts to other nodes do not block
 * request processing.
 */
class StreamletNodeStrict : public Streamlet::Service {
public:
    /*
     * Params
     *   id: Index in peers that is the ID of this node
     *   peers: Vector of peers in the including the local node
     *   priv: Private key of local node
     *   pub: Public key of local node
     *   epoch_len: Duration of the epoch in milliseconds
     *   client_app: Distributed application implementing a ReplicatedStateMachine
     */
    StreamletNodeStrict(
        uint32_t id,
        const std::vector<Peer> &peers,
        const Key &priv,
        const gpr_timespec &epoch_len,
        ReplicatedStateMachine &client_app
    );

    ~StreamletNodeStrict();

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
    void Run(gpr_timespec epoch_sync);

private:
    void broadcast_vote(const Proposal* proposal, const std::string &hash);

    void record_proposal(const Proposal *proposal, uint64_t epoch);

    // See long comment above the same method in StreamletNodeGST. The functionality
    // there is not needed in the strict model, but the same function can be used
    // and is copied here to reduce implementation effort.
    void notarize_block(
        const Block &note_block,
        const std::string &note_hash,
        uint64_t note_epoch,
        uint64_t par_epoch
    );

    NetworkInterposer network;

    CryptoManager crypto;

    ReplicatedStateMachine &client;

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
    const gpr_timespec epoch_duration;

    // Current epoch number
    std::atomic_uint64_t epoch_counter;
};

StreamletNodeStrict::StreamletNodeStrict(
    uint32_t id,
    const std::vector<Peer> &peers,
    const Key &priv,
    const gpr_timespec &epoch_len,
    ReplicatedStateMachine &client_app)
    : network{peers, id},
    crypto{peers, priv, peers.at(id).key},
    client{client_app},
    max_chainlen{0},
    genesis_block{0},
    local_addr{peers.at(id).addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    note_threshold{((num_peers * 2) / 3) + (num_peers % 3)},
    epoch_duration{epoch_len},
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

StreamletNodeStrict::~StreamletNodeStrict() {

}

grpc::Status StreamletNodeStrict::NotifyVote(
    grpc::ServerContext* context,
    const Vote* vote,
    Response* response
) {
    const uint64_t b_epoch = vote->epoch();
    const std::string& b_hash = vote->hash();
    const uint32_t voter = vote->node();

    if (!crypto.verify_signature(voter, b_hash, vote->signature())) {
        std::cout << "Warning: Signature verification failed on vote for block " << b_epoch
            << " by node " << voter <<  ", discarding message" << std::endl;
        return grpc::Status::OK; // discard
    }

    // Cleanup the block
    bool remove = false;

    // The number of votes including the vote from the message being
    // processed if it is valid
    uint32_t votes = 0;

    Candidate *cand = nullptr;

    candidates_m.lock();
    std::unordered_map<uint64_t, Candidate*>::iterator iter
            = candidates.find(b_epoch);

    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    if (b_epoch > cur_epoch) {
        candidates_m.unlock(); // discard and ignore or report error
    } else if (b_epoch == cur_epoch) {
        if (iter == candidates.end()) {
            cand = new Candidate{num_peers};
            candidates.emplace(b_epoch, cand);
        } else {
            cand = iter->second;
        }
        cand->ref_count.fetch_add(1, std::memory_order_relaxed);

        candidates_m.unlock();

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
    } else {
        // Since cur_epoch > b_epoch, if cand is not found, the proposal
        // was not seen in time and any prior Candidate was cleaned up
        if (iter != candidates.end()) {
            cand = iter->second;

            // If cand is found but no proposal has been seen, remove and delete the Candidate
            // TODO: Consider whether this can be factored outside candidates_m
            cand->m.lock();
            if (cand->hash.empty()) {
                candidates.erase(iter);
                cand->removed = true;
                remove = (cand->ref_count.load(std::memory_order_relaxed) == 0);
            } else if (cand->hash == b_hash && cand->voters[voter] != true) {
                cand->voters[voter] = true;
                votes = ++cand->votes;
            }
            cand->m.unlock();

            candidates_m.unlock();
        } else {
            candidates_m.unlock();
        }
    }

    // cand not nullptr if remove is true or votes > 0
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

    // If the block has obtained enough votes for notarization but its epoch has
    // passed by the the time the leader's proposal is seen, then we can consider
    // ourselves faulty and start shutdown. In particular, we know this is the case
    // if remove == true is ever seen in the else branch above since any proposal
    // that is waiting 

    return grpc::Status::OK;
}

grpc::Status StreamletNodeStrict::ProposeBlock(
    grpc::ServerContext* context,
    const Proposal* proposal,
    Response* response
) {
    const uint64_t b_epoch = proposal->block().epoch();
    const uint64_t b_parent = proposal->block().parent();
    const uint32_t proposer = proposal->node();

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);
    if (!crypto.verify_signature(proposer, hash, proposal->signature())) {
        std::cout << "Warning: Signature verification failed for block " << b_epoch
            << " proposed by node " << proposer <<  ", discarding message" << std::endl;
        return grpc::Status::OK; // discard message
    }

    // Check that the block extends the longest chain. There are several cases
    // depending on whether the parent is the genesis block, or the parent
    // is not the genesis block and has a known chain index, and whether this
    // chain index less than the known maximum chain length.
    //
    // In the strict model, we assume a synchronous network and only allow
    // candidates to be considered within the epoch, following the protocol
    // more closely and ignoring the implications of a GST. Hence, if the
    // parent has not been seen, we cannot vote on the block and moreover
    // may consider ourselves faulty.
    successors_m.lock();
    std::unordered_map<uint64_t, ChainElement*>::iterator iter
        = successors.find(b_parent);

    if (iter == successors.end()) {
        successors_m.unlock();
        return grpc::Status::OK; // discard
    } else if ((b_parent == 0 && max_chainlen > 0)
        || (b_parent != 0 && iter->second->index == 0)
        || (b_parent != 0 && iter->second->index != 0 && iter->second->index < max_chainlen)) {
            successors_m.unlock();
            std::cout << "Warning: Received proposal for block " << b_epoch
                << " without having first seen proposal for parent block "
                << proposal->block().parent() << ", discarding message" << std::endl;
            return grpc::Status::OK; // discard
    } else {
        successors_m.unlock();
    }

    // Check that the block is a valid extension according to the application
    // by invoking a callback on the ReplicatedStateMachine.
    if (!client.ValidateTransactions(proposal->block().txns(), proposal->block().epoch())) {
        std::cout << "Warning: Client rejected proposal for epoch " << b_epoch
            << ", discarding message" << std::endl;
        return grpc::Status::OK; // discard message
    }

    bool remove = false;
    uint32_t votes = 0;
    Candidate *cand = nullptr;

    candidates_m.lock();

    // The epoch is read while inside the lock to ensure that cur_epoch is
    // ordered so that b_epoch == cur_epoch cannot be seen in ProposeBlock
    // after b_epoch < cur_epoch has been seen in NotifyVote. Otherwise,
    // a Candidate could be allocated and inserted into candidates below
    // but never garbage collected if it doesn't receive votes.
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);

    if (proposer != crypto.hash_epoch(cur_epoch)) {
        std::cout << "Warning: Received proposal from wrong leader in epoch "
            << cur_epoch << ", discarding message" << std::endl;
        candidates_m.lock(); // discard message
    } else if (b_epoch == cur_epoch) {
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

        // An empty hash means the proposal has not yet been seen. If it has been
        // seen, then this is a duplicate message that must be dropped.
        cand->m.lock();
        if (cand->hash.empty()) {
            cand->hash = hash;

            // In the strict model, votes and proposal must arrive in the associated epoch
            // and the parent must already have been notarized for a block to be added to
            // a chain. Thus, if the checks above pass, then both the proposer's and local
            // vote should be recorded.
            votes = 2;
            cand->voters[local_id] = true;
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
    } else {
        std::cout << "Warning: Received proposal for epoch " << b_epoch
            << "in epoch " << cur_epoch << ", discarding message" << std::endl;
        candidates_m.unlock(); // discard message
    }

    // Between candidates_m.unlock() and cand.m.lock() in the if branch above,
    // it is possible that the epoch advances and a call to NotifyVote for the
    // same block sees that b_epoch < cur_epoch. Since cand.hash is still empty,
    // NotifyVote will think that the proposal has failed to arrive in time and
    // mark the block for removal. Since that vote will be lost, and any yielded
    // concurrent calls to NotifyVote with b_epoch == cur_epoch also wake up
    // expecting the block to be removed, the proposal must be discarded here.
    if (remove) {
        delete cand;
    } else if (votes >= note_threshold) {
        notarize_block(proposal->block(), hash, b_epoch, b_parent);
    }

    // votes is 0 when the message is a duplicate or unanticipated, echo otherwise
    if (!remove && votes > 0) {
        network.broadcast(*proposal, &req_queue);

        // In the strict model, b_epoch == cur_epoch if votes > 0, so broadcast the local vote
        broadcast_vote(proposal, hash);
    }

    return grpc::Status::OK;
}

void StreamletNodeStrict::notarize_block(
    const Block &note_block,
    const std::string &note_hash,
    uint64_t note_epoch,
    uint64_t par_epoch
) {
    notification_m.lock();
    client.TransactionsNotarized(note_block.txns(), note_epoch);
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

            client.TransactionsFinalized(prev_finalized->block.txns(), prev_finalized->block.epoch());

        } while (prev_finalized != new_finalized);
    }

    notification_m.unlock();
}

void StreamletNodeStrict::broadcast_vote(const Proposal* proposal, const std::string &hash) {
    Vote v;
    v.set_node(local_id);
    v.set_parent(proposal->block().parent());
    v.set_epoch(proposal->block().epoch());
    v.set_hash(hash);

    crypto.sign_sha256(hash, v.mutable_signature());

    network.broadcast(v, &req_queue);
}

void StreamletNodeStrict::record_proposal(const Proposal *proposal, uint64_t epoch) {
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

void StreamletNodeStrict::Run(gpr_timespec epoch_sync) {
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

    // Client should only begin tracking time after the initial sync is complete
    client.BeginTime();

    while (status != grpc::CompletionQueue::NextStatus::SHUTDOWN) {
        // Always check if the epoch advanced
        gpr_timespec t_now = gpr_now(GPR_CLOCK_MONOTONIC);

        if (gpr_time_cmp(gpr_time_sub(t_now, epoch_sync), epoch_duration) >= 0) {
            epoch_sync = gpr_time_add(epoch_sync, epoch_duration);

            // Get delay after updating epoch_sync
            // gpr_timespec delay = gpr_time_sub(t_now, epoch_sync);
            // std::cout << "Timeout delay: " << delay.tv_nsec << std::endl;

            // Since the increment specifies relaxed memory order, be careful
            // that no code below this statement changes other data shared among
            // threads that must be sequenced with epoch_counter.
            uint64_t cur_epoch = epoch_counter.fetch_add(1, std::memory_order_relaxed);

            cur_epoch++; // Must be incremented because the atomic op returns the previous value

            if (crypto.hash_epoch(cur_epoch) == local_id) {
                // run leader logic
                Proposal p;

                p.set_node(local_id);
                // todo: add signature when crypto library done
                // p.set_signature();
                p.mutable_block()->set_parent(max_chainlen);

                successors_m.lock();
                p.mutable_block()->set_phash(last_chainlen->hash);
                successors_m.unlock();

                p.mutable_block()->set_epoch(cur_epoch);

                client.GetTransactions(p.mutable_block()->mutable_txns(), cur_epoch);

#ifdef PRINT_TRANSACTIONS
                std::cout << "Epoch " << cur_epoch << ", leader " << local_id
                    << ": " << p.block().txns() << std::endl;
#endif

                network.broadcast(p, &req_queue);

                // Broadcast first then propose locally to overlap the network requests
                // with local exection. record_proposal is designed to be safe to call
                // with the possiblity of from remote nodes echoing the proposal so that
                // ProposeBlock is called before or concurrently with record_proposal.
                record_proposal(&p, cur_epoch);
            }
        }

        // Nothing to process if the status is TIMEOUT. Otherwise, tag
        // is a completed async request that must be cleaned up.
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
        std::cout << "Usage: StreamletNodeV1 <sync_time> <epoch_len> <config_file> <local_id>\n"
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

    gpr_time_init();

    ThroughputLossStateMachine rsm{
        id,
        static_cast<uint32_t>(peers.size()),
        gpr_time_from_millis(1000, GPR_TIMESPAN)
    };

    StreamletNodeStrict service{
        id,
        peers,
        privkey,
        gpr_time_from_millis(epoch, GPR_TIMESPAN),
        rsm
    };

    // Run this as close to service.Run() as possible
    gpr_timespec sync_start;
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