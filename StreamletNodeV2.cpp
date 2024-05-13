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
    /*
     * Params
     *   id: Index in peers that is the ID of this node
     *   peers: Vector of peers in the including the local node
     *   priv: Private key of local node
     *   pub: Public key of local node
     *   epoch_len: Duration of the epoch in milliseconds
     */
    StreamletNodeV2(
        uint32_t id,
        const std::vector<Peer> &peers,
        const Key &priv,
        const std::chrono::milliseconds &epoch_len
    );

    ~StreamletNodeV2();

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
    NetworkInterposer network;

    CryptoManager crypto;

    // Completion queue for outbound RPCs
    grpc::CompletionQueue req_queue;

    // Blocks that have been seen or proposed and that are awaiting votes
    std::unordered_map<uint64_t, ChainElement*> candidates;

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

    // Number of votes at which a block is notarized
    const uint32_t note_threshold;

    // Duration of each epoch
    const system_clock::duration epoch_duration;

    // Current epoch number
    std::atomic_uint64_t epoch_counter;
};

StreamletNodeV2::StreamletNodeV2(
    uint32_t id,
    const std::vector<Peer> &peers,
    const Key &priv,
    const std::chrono::milliseconds &epoch_len)
    : network{peers},
    crypto{peers, priv, peers.at(id).key},
    genesis_block{static_cast<uint32_t>(peers.size())},
    local_addr{peers.at(id).addr},
    local_id{id},
    num_peers{static_cast<uint32_t>(peers.size())},
    note_threshold{((num_peers + 2) / 3) * 2},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch_counter{0} {

    // Create entry for dependents of genesis block
    successors.emplace(0, std::list<uint64_t>{});
}

StreamletNodeV2::~StreamletNodeV2() {

}

grpc::Status StreamletNodeV2::NotifyVote(
    grpc::ServerContext* context,
    const Vote* vote,
    Response* response
) {
    const uint64_t b_epoch = vote->epoch();
    const std::string& b_hash = vote->hash();
    const uint32_t voter = vote->node();

    if (!crypto.verify_signature(voter, b_hash, vote->signature())) {
        return grpc::Status::OK; // discard
    }

    // Cleanup the block
    bool remove = false;

    // The number of votes including the vote from the message being
    // processed, if it is valid
    uint32_t votes = 0;

    ChainElement *elem = nullptr;

    // In the following cases, apply notarization procedure if and only if
    // remove == false and votes == 2/3 majority. votes becomes non-zero
    // only when the newly recorded vote puts the block at a 2/3 majority.
    // Duplicate messages do not set votes and hence will not indicate
    // notarization a second time.

    candidates_m.lock();
    std::unordered_map<uint64_t, ChainElement*>::iterator iter
            = candidates.find(b_epoch);

    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);
    if (b_epoch > cur_epoch) {
        candidates_m.unlock(); // discard and ignore or report error
    } else if (b_epoch == cur_epoch) {
        if (iter == candidates.end()) {
            elem = new ChainElement{num_peers};
            candidates.emplace(b_epoch, elem);
        } else {
            elem = iter->second;
        }
        elem->ref_count.fetch_add(1, std::memory_order_relaxed);

        candidates_m.unlock();

        elem->m.lock();
        // Empty hash means the proposal has not yet been seen
        if (elem->hash.empty()) {
            elem->vote_hashes.emplace(voter, b_hash);
        } else if (elem->hash == b_hash && elem->voters[voter] != true) {
            elem->voters[voter] = true;
            votes = ++elem->votes;
        }
        remove = (elem->removed
            && (elem->ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        elem->m.unlock();

        if (remove) {
            delete elem;
        } else if (votes == note_threshold) {

        }
    } else {
        // Since cur_epoch > b_epoch, if elem is not found, the proposal
        // was not seen in time and any prior ChainElem was cleaned up
        if (iter != candidates.end()) {
            elem = iter->second;

            // If elem is found, but no proposal has been seen, remove it
            // TODO: Consider whether this can be factored outside candidates_m
            elem->m.lock();
            if (elem->hash.empty()) {
                candidates.erase(iter);
                elem->removed = true;
                remove = (elem->ref_count.load(std::memory_order_relaxed) == 0);
            } else if (elem->hash == b_hash && elem->voters[voter] != true) {
                elem->voters[voter] = true;
                votes = ++elem->votes;
            }
            elem->m.unlock();

            candidates_m.unlock();

            if (remove) {
                delete elem;
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
    const Proposal* proposal,
    Response* response
) {
    const uint64_t b_epoch = proposal->block().epoch();
    const uint32_t proposer = proposal->node();

    std::string hash;
    crypto.hash_block(&proposal->block(), hash);
    if (!crypto.verify_signature(proposer, hash, proposal->signature())) {
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
    ChainElement *elem = nullptr;

    candidates_m.lock();

    // The epoch is read while inside the lock to ensure that cur_epoch is
    // ordered across so that b_epoch == cur_epoch cannot be seen in ProposeBlock
    // after b_epoch < cur_epoch has been seen in NotifyVote
    const uint64_t cur_epoch = epoch_counter.load(std::memory_order_relaxed);

    if (proposer != crypto.hash_epoch(cur_epoch)) {
        candidates_m.lock(); // discard message
    } else if (b_epoch == cur_epoch) {
        std::unordered_map<uint64_t, ChainElement*>::iterator iter
            = candidates.find(b_epoch);
        if (iter == candidates.end()) {
            elem = new ChainElement{num_peers};
            candidates.emplace(b_epoch, elem);
        } else {
            elem = iter->second;
        }
        elem->ref_count.fetch_add(1, std::memory_order_relaxed);

        candidates_m.unlock();

        // An empty hash means the proposal has not yet been seen. If it has been
        // seen, then this is a duplicate message that must be dropped.
        elem->m.lock();
        if (elem->hash.empty()) {
            elem->hash = hash;
            votes = 1; // Count proposer's vote here

            elem->voters[proposer] = true;
            for (std::pair<const uint32_t, std::string> &p : elem->vote_hashes) {
                if (p.second == hash) {
                    elem->voters[p.first] = true;
                    votes++;
                }
            }
            elem->votes = votes;

            elem->vote_hashes.clear();

            valid = true;
        }
        remove = (elem->removed
            && (elem->ref_count.fetch_sub(1, std::memory_order_relaxed) == 1));
        elem->m.unlock();
    } else {
        candidates_m.unlock(); // discard message
    }

    // Between candidates_m.unlock() and elem.m.lock() in the if branch above,
    // it is possible that the epoch advances and a call to NotifyVote for the
    // same block sees that b_epoch < cur_epoch. Since elem.hash is still empty,
    // NotifyVote will think that the proposal has failed to arrive in time and
    // mark the block for removal. Since that vote will be lost, and any yielded
    // concurrent calls to NotifyVote with b_epoch == cur_epoch also wake up
    // expecting the block to be removed, the proposal must be discarded here.
    if (remove) {
        delete elem;
    } else if (valid && votes == note_threshold) {
        // Link pending chain elements and check for finalization
    }

    return grpc::Status::OK;
}

void StreamletNodeV2::Run(system_clock::time_point epoch_sync) {
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

    StreamletNodeV2 service{
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