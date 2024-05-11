#include "grpc/grpc.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"

#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/completion_queue.h"

#include "streamlet.grpc.pb.h"

#include "structs.h"
#include "setup.h"

#include <iostream>
#include <mutex>

/*
 * Implements an extension to the Streamlet protocol in which votes are
 * allowed to arrive after the epoch in which a block is proposed. However,
 * epochs still track physical time, and in particular, a new block cannot
 * be proposed until the duration of physical time corresponding to an
 * epoch has elapsed, even if the block proposed in a prior epoch has been
 * notarized.
 *
 * The service implements the RPC server using the synchronous interface
 * and acts as an RPC client using the asynchronous interface so that
 * broadcasts to other nodes do not block request processing.
 */
class StreamletNodeV2 : public Streamlet::Service {
public:
    using std::chrono::system_clock;

    /*
     * Params
     *   id: Index in peers that is the ID of this node
     *   peers: List of peers in the including the local node
     *   priv: Private key of local node
     *   pub: Public key of local node
     *   epoch_len: Duration of the epoch in milliseconds
     */
    StreamletNodeV2(
        uint32_t id,
        const std::list<Peer> &peers,
        const Key &priv,
        const Key &pub,
        const std::chrono::milliseconds &epoch_len
    );

    ~StreamletNodeV2();

    grpc::Status NotifyVote(
        grpc::ServerContext* context,
        const Vote* request,
        Response* response
    ) override;

    grpc::Status ProposeBlock(
        grpc::ServerContext* context,
        const Vote* request,
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
    NetworkInterposer network;

    CryptoManager crypto;

    // Completion queue for outbound RPCs
    grpc::CompletionQueue req_queue;

    // Blocks that have been seen or proposed and that are awaiting votes
    std::unordered_map<uint64_t, ChainElement> candidates;

    // Mutex for candidates. Short reads in common case so better not
    // having reader/writer lock.
    std::mutex candidates_m;

    // Mapping from a block to its dependents so that the chain can
    // be constructed with later blocks that were notarized first and
    // were waiting on a given earlier block
    std::unordered_map<uint64_t, std::list<uint64_t>> successors;

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

    // Duration of each epoch
    const system_clock::duration epoch_duration;

    // Current epoch number
    std::atomic_uint64_t epoch_counter;
};

StreamletNodeV2::StreamletNodeV2(
    uint32_t id,
    const std::list<Peer> &peers,
    const Key &priv,
    const Key &pub,
    const std::chrono::milliseconds &epoch_len)
    : network{peers},
    crypto{peers, priv, pub},
    genesis_block{0, 0, std::vector<bool>{}, Block{}},
    local_addr{peers[local_id].addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch_counter{0} {

    // Create entry for dependents of genesis block
    successors.push_back(0, std::list{});
}

StreamletNodeV2::~StreamletNodeV2() {

}

grpc::Status StreamletNodeV2::NotifyVote(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {
    const uint64_t b_epoch = vote->epoch();

    if (!crypto.verify_signature(vote->node(), vote->hash(), vote->signature())) {
        return grpc::Status::OK; // discard
    }

    // Cleanup the block
    bool remove = false;

    // The number of votes including the vote from the message being
    // processed, if it is valid
    uint32_t votes = 0;

    // In the following cases, apply notarization procedure if and only if
    // remove == false and votes == 2/3 majority. votes becomes non-zero
    // only when the newly recorded vote puts the block at a 2/3 majority.
    // Duplicate messages do not set votes and hence will not indicate
    // notarization a second time.

    candidates_m.lock();
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    if (b_epoch > cur_epoch) {
        candidates_m.unlock(); // discard and ignore or report error
    } else if (b_epoch == cur_epoch) {
        // operator[] will construct the entry if it does not exist
        ChainElement &elem = candidates[b_epoch];
        elem.ref_count.fetch_add(1, std::memory_order_relaxed);

        candidates_m.unlock();

        elem.m.lock();
        // Empty hash means the proposal has not yet been seen
        if (elem.hash.empty()) {
            elem.vote_hashes.emplace_back(vote->node(), vote->hash());
        } else if (elem.hash == vote->hash() && elem.voters[i] != true) {
            elem.voters[i] = true;
            votes = elem.voters.count();
        }
        remove = (elem.removed
            && (elem.ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        elem.m.unlock();

        if (remove) {
            // delete
        } else if (votes == note_threshold) {

        }
    } else { // b_epoch < cur_epoch
        std::unordered_map<uint64_t, ChainElement>::iterator iter
            = candidates.find(b_epoch);

        // Since cur_epoch > b_epoch, if elem is not found, the proposal
        // was not seen in time and any prior ChainElem was cleaned up
        if (iter != candidates.end()) {
            ChainElement &elem = iter.second;

            // If elem is found, but no proposal has been seen, remove it
            // TODO: Consider whether this can be factored outside candidates_m
            elem.m.lock();
            if (elem.hash.empty()) {
                candidates.erase(iter);
                elem.removed = true;
                remove = (elem.ref_count.load(std::memory_order_relaxed) == 0);
            } else if (elem.hash == vote->hash() && elem.voters[i] != true) {
                elem.voters[i] = true;
                votes = elem.voters.count();
            }
            elem.m.unlock();

            candidates_m.unlock();

            if (remove) {
                // delete
            } else if (votes == note_threshold) {

            }

        } else {
            candidates_m.unlock();
        }

    }

    // If the block has obtained enough votes for notarization but its epoch has
    // passed by the the time the leader's proposal is seen, then we can consider
    // ourselves faulty and start shutdown. In particular, we know this is the case
    // if remove == true is ever seen in the else branch above since any proposal
    // that is waiting 

    return grpc::Status::OK;
}

grpc::Status StreamletNodeV2::ProposeBlock(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {
    const uint64_t b_epoch = proposal->block()->epoch();

    if (proposal->node() != crypto.hash_epoch(cur_epoch)) {
        return grpc::Status::OK; // discard message
    }

    std::string hash;
    crypto.hash_block(proposal->block(), hash);
    if (!crypto.verify_signature(proposal->node(), hash, proposal->signature())) {
        return grpc::Status::OK; // discard message
    }

    // Check that the block extends the longest chain
    // if () {
        
    // }

    // Check that the block is a valid extension according to the application
    // by invoking a callback on the ReplicatedStateMachine.
    // if () {
        
    // }

    bool valid = false;
    bool remove = false;
    uint32_t votes = 0;

    candidates_m.lock();

    // The epoch is read while inside the lock to ensure that cur_epoch is
    // ordered across so that b_epoch == cur_epoch cannot be seen in ProposeBlock
    // after b_epoch < cur_epoch has been seen in NotifyVote
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);

    if (b_epoch == cur_epoch) {
        // operator[] will construct the entry if it does not exist
        ChainElement &elem = candidates[b_epoch];
        elem.ref_count.fetch_add(1, std::memory_order_relaxed);
        candidates_m.unlock();

        // An empty hash means the proposal has not yet been seen. If it has been
        // seen, then this is a duplicate message that must be dropped.
        elem.m.lock();
        if (elem.hash.empty()) {
            elem.hash = hash;
            elem.voters.resize(num_peers);

            for (std::pair<const uint32_t, std::string> &p : elem.vote_hashes) {
                if (p.second == hash) {
                    elem.voters[p.first] = true;
                    votes++;
                }
            }

            elem.vote_hashes.clear();

            valid = true;
        }
        remove = (elem.removed
            && (elem.ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        elem.m.unlock();
    } else {
        candidates_m.unlock();
    }

    if (remove) {

    } else if (valid && votes == note_threshold) {

    }

    // If valid is false when the branches above converge, the block is discarded.
    // Otherwise, it has been verified and notarization and finalization can be checked.
    if (valid && votes == note_threshold) {
        // Link pending chain elements and check for finalization
    }

    return grpc::Status::OK;
}

void StreamletNodeV2::Run(system_clock::time_point epoch_sync) {
    // Build server and run
    std::string server_address{ local_addr };

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    // Poll for completed async requests and monitor epoch progress
    grpc::CompletionQueue::NextStatus status;
    void *tag;
    bool ok;
    status = req_queue.AsyncNext(&tag, &ok, epoch_sync)
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
    // read command line: sync_time config_file local_id
    // parse config into std::list<Peer>
    int status = 0;

    StreamletNodeV2 service{

    };

    std::chrono::system_clock::time_point sync_start;
    status = sync_time(argv[1], sync_start);

    if (status == 1) {
        std::cerr << "Start time must be a valid HH:MM:SS" << std::endl;
        return 1;
    } else if (status == 2) {
        std::cerr << "Start time already passed" << std::endl;
        return 1;
    }

    service.Run(sync_start);

    return 0;
}