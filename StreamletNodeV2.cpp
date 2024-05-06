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
     * Construct a streamlet node
     *   id: Index in peers that is the ID of this node
     *   peers: List of peers in the including the local node
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

    void Run();

private:
    NetworkInterposer network;

    CryptoManager crypto;

    // Completion queue for outbound RPCs
    grpc::CompletionQueue req_queue;

    // Genesis block
    const ChainElement genesis_block;

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
    std::string server_address{ "0.0.0.0:50051" };

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    // Process finished RPCs and alarms on the main thread.
    // Finished RPCs only need to be cleaned up since the RPC
    // service is not expected to return any information
    // in the current implementation. Alarms ensure that the
    // thread unblocks from the wait on the completion queue so
    // that it does not miss the start of an epoch if there are
    // no messages occurring from remote peers

    Alarm alarm;
    alarm.set(req_queue, epoch_sync, &alarm);

    void *tag;
    bool ok;
    while (req_queue.Next(&tag, &ok)) {
        // Always check if the epoch advanced
        system_clock::time_point t_now = system_clock::now();
        if ((t_now - epoch_sync) >= epoch_duration) {
            epoch_sync += epoch_duration;
            epoch++;

            alarm.set(req_queue, epoch_sync, &alarm);

            // if (crypto.hash_epoch(epoch) == local_id)
            //     run leader logic
        } else if (tag == &alarm) {
            // If for some reason the alarm expired a tad before the
            // next epoch, reschedule to ensure that the call unblocks
            // and the epoch will be updated in a timely manner
            alarm.set(req_queue, epoch_sync + epoch_duration, &alarm);
        }

        // Nothing to process if the dequeued tag points to the alarm.
        // Otherwise, tag is a completed async request that must be cleaned up.
        if (tag != &alarm) {
            NetworkInterposer::Pending *req = static_cast<NetworkInterposer::Pending*>(tag);

            // optionally check status and response (currently an empty struct)

            // cleanup code here
        }
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