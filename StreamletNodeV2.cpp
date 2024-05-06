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

    // Genesis block
    const ChainElement genesis_block;

    // Address of this server
    const std::string local_addr;

    // The ID of this node
    const uint32_t local_id;

    // Duration of each epoch
    const system_clock::duration epoch_duration;

    // Epoch counter
    uint64_t epoch;
};

StreamletNodeV2::StreamletNodeV2(
    uint32_t id,
    const std::list<Peer> &peers,
    const Key &priv,
    const Key &pub,
    const std::chrono::milliseconds &epoch_len)
    : network{peers},
    crypto{peers, priv, pub},
    genesis{0, 0, std::vector<bool>{}, Block{}},
    local_addr{peers[local_id].addr},
    local_id{id},
    epoch_duration{std::chrono::duration_cast<system_clock::duration>(epoch_len)},
    epoch{0} {

}

StreamletNodeV2::~StreamletNodeV2() {

}

grpc::Status StreamletNodeV2::NotifyVote(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {
    return grpc::Status::OK;
}

grpc::Status StreamletNodeV2::ProposeBlock(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {
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
            epoch++;

            // if (crypto.hash_epoch(epoch) == local_id)
            //     run leader logic
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