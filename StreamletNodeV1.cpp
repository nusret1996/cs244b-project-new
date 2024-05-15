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
        const std::chrono::milliseconds &epoch_len
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
    void notarize_block(const Block &note_block, uint32_t note_epoch, uint32_t par_epoch);

    NetworkInterposer network;

    CryptoManager crypto;

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
    std::unordered_map<uint64_t, ChainElement> successors;

    // Length of longest notarized chain, also guarded by successors_m
    uint64_t max_chainlen;

    // Mutex for successors
    std::mutex successors_m;

    // Genesis block
    const ChainElement genesis_block;

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
    const std::chrono::milliseconds &epoch_len)
    : network{peers},
    crypto{peers, priv, peers.at(id).key},
    max_chainlen{0},
    genesis_block{},
    local_addr{peers.at(id).addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    note_threshold{((num_peers + 2) / 3) * 2},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch_counter{0} {

    // Create entry for dependents of genesis block
    successors.emplace(0, ChainElement{});
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

    candidates_m.lock();
    std::unordered_map<uint64_t, Candidate*>::iterator iter
            = candidates.find(b_epoch);

    // discard and ignore or report error if b_epoch > cur_epoch
    if (b_epoch <= cur_epoch) {
        if (iter == candidates.end()) {
            cand = new Candidate{num_peers};
            candidates.emplace(b_epoch, cand);
        } else {
            cand = iter->second;
        }

        cand->ref_count.fetch_add(1, std::memory_order_relaxed); 
    }
    candidates_m.unlock();

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
        } else if (votes == note_threshold) {
            
        }

        // votes is 0 exactly when the message is a duplicate
        if (!remove && votes > 0) {
            // echo vote with network.broadcast()
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
        return grpc::Status::OK; // discard message
    }

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);
    if (!crypto.verify_signature(proposer, hash, proposal->signature())) {
        return grpc::Status::OK; // discard message
    }

    // Check that the block extends the longest chain. Since messages can be delayed
    // and the proposal might not have been seen by the time a block is notarized,
    // it is not always possible to check this. A propsal can be definitely ruled out
    // if its parent's index is known and is less than the maximum notarized chain length.
    // Since a ChainElement's index is 0 when its position is not known, and the genesis
    // block's index is also 0, this definite condition is checked in two cases, one for
    // when the parent block is the genesis block and one for when it is not.
    successors_m.lock();
    std::unordered_map<uint64_t, ChainElement>::iterator iter
        = successors.find(b_parent);

    if (iter != successors.end() &&
        ((b_parent != 0 && iter->second.index != 0 && iter->second.index < max_chainlen)
            || (b_parent == 0 && max_chainlen > 0))) {
                successors_m.unlock();
                return grpc::Status::OK; // discard
    } else {
        successors_m.unlock();    
    }

    // Check that the block is a valid extension according to the application
    // by invoking a callback on the ReplicatedStateMachine.
    // if () {

    // }

    bool remove = false;
    uint32_t votes = 0;
    Candidate *cand = nullptr;

    candidates_m.lock();
    // discard and ignore or report error if b_epoch > cur_epoch
    if (b_epoch <= cur_epoch) {
        std::unordered_map<uint64_t, Candidate*>::iterator iter
            = candidates.find(b_epoch);

        if (iter == candidates.end()) {
            cand = new Candidate{num_peers};
            candidates.emplace(b_epoch, cand);
        } else {
            cand = iter->second;
        }

        cand->ref_count.fetch_add(1, std::memory_order_relaxed);        
    }
    candidates_m.unlock();

    if (cand != nullptr) {
        // An empty hash means the proposal has not yet been seen. If it has been
        // seen, then this is a duplicate or invalid message and must be ignored.
        cand->m.lock();
        if (cand->hash.empty()) {
            cand->hash = hash;

            votes = 2; // Count proposer's vote and our vote here

            cand->voters[proposer] = true;
            cand->voters[local_id] = true;
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
        } else if (votes == note_threshold) {
            // can lock candidates map, remove from map, add entry to some "finished" set, and then unlock
            // then call notarize_block() with the Candidate's block
            // then lock the Candidate, set the remove flag, record ref_count.load(std::memory_order_relaxed),
            // and then unlock, and finally deallocate the candidate if ref_count was zero
        }

        // votes is 0 exactly when the message is a duplicate
        if (!remove && votes > 0) {
            // echo proposal with network.broadcast()

            // send out our vote with network.broadcast()
        }
    }

    return grpc::Status::OK;
}

/*
 * notarize_block must be called only once for a given block/epoch id.
 */
void StreamletNodeV1::notarize_block(const Block &note_block, uint32_t note_epoch, uint32_t par_epoch) {
    std::list<ChainElement*> queue;

    successors_m.lock();

    // std::pair<std::unordered_map<uint64_t, ChainElement>::iterator, bool> new_ins =
    //     successors.emplace(note_epoch, ChainElement{});

    ChainElement &new_elem = successors[note_epoch];
    if (new_elem.epoch == 0) {
        new_elem.epoch = note_epoch; // ChainElement was newly constructed
        new_elem.block.CopyFrom(note_block);
    } else if (!new_elem.block.IsInitialized()) {
        new_elem.block.CopyFrom(note_block); // ChainElement already existed but block was previously not notarized
    }

    ChainElement &p_elem = successors[par_epoch];
    if (p_elem.epoch == 0 && par_epoch != 0) {
        p_elem.epoch = par_epoch; // parent ChainElement was also constructed
    }

    p_elem.links.push_back(&new_elem);

    // If the parent is already linked into the chain, or the parent is the genesis
    // block, then set the chain index of new_elem and look for any children
    // that can be linked and check if they extend the longest chain at their position.
    if (p_elem.index != 0 || par_epoch == 0) {
        new_elem.index = p_elem.index + 1;

        queue.push_back(&new_elem);

        // Run a BFS to relink previously notarized blocks
        std::list<ChainElement*>::iterator elem = queue.begin();
        while (elem != queue.end()) {
            for (ChainElement *child : (*elem)->links) {
                child->index = (*elem)->index + 1;
                queue.push_back(child);
            }
            elem++;
        }

        // Because queue is expanded in order of increasing BFS depth, the
        // last element must contain the length of the longest notarized
        // chain, although this chain is not necessarily unique
        max_chainlen = queue.back()->index;
    }

    // At this point we can backtrace over queue from all elements at the
    // end that are at index max_chainlen and any element that isn't
    // in a longest chain can be removed. The successors map owns the
    // ChainElement so cleanup only requires erasing the entry from the map.

    successors_m.unlock();
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
            }
        }

        // Nothing to process if the status is TIMEOUT. Otherwise, tag
        // is a completed async request that must be cleaned up.
        if (status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            NetworkInterposer::Pending *req = static_cast<NetworkInterposer::Pending*>(tag);

            // optionally check status and response (currently an empty struct)

            // cleanup code here
        }

        status = req_queue.AsyncNext(&tag, &ok, epoch_sync);
    }

    server->Shutdown();
    server->Wait();
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

    StreamletNodeV1 service{
        id,
        peers,
        privkey,
        std::chrono::milliseconds{epoch}
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