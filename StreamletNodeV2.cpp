#include "structs.h"

#include "grpc/grpc.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"

#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/completion_queue.h"

#include "streamlet.grpc.pb.h"

#include "structs.h"

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
     * Construct a streamlet node
     *   id: Index in peers that is the ID of this node
     *   peers: List of peers in the including the local node
     */
    StreamletNodeV2(uint32_t id, const std::list<Peer> &peers, const Key &priv, const Key &pub);

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

    // The ID of this node
    const uint32_t local_id;

    uint64_t epoch;
};

StreamletNodeV2::StreamletNodeV2(
    uint32_t id,
    const std::list<Peer> &peers,
    const Key &priv,
    const Key &pub)
    : network{peers},
    crypto{peers, priv, pub},
    local_id{id},
    epoch{0} {

}

StreamletNodeV2::~StreamletNodeV2() {

}

grpc::Status StreamletNodeV2::NotifyVote(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {

}

grpc::Status StreamletNodeV2::ProposeBlock(
    grpc::ServerContext* context,
    const Vote* request,
    Response* response
) {

}

void StreamletNodeV2::Run() {
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
    
    while (req_queue.Next()) {
        // Check for epoch change here

        // If epoch advanced, hash it, and check if we are the leader
    }

    server->Shutdown();
}