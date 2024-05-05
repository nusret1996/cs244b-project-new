#include <list>
#include <vector>

#include "grpcpp/completion_queue.h"

#include "streamlet.grpc.pb.h"

#include "struct.h"

/*
 * Abstracts away broadcasting and allows a NetworkInterposer
 * gRPC service to be inserted into the system to simulate varoius,
 * including adversarial, network conditions.
 */
class NetworkInterposer {
public:
    /*
     * Structure representing a pending RPC for broadcasting block proposals,
     * when acting as leader, or for implicit echoing, when acting as follower.
     */
    struct Pending {
        ClientContext context;
        Response resp;
        grpc::Status status;
    }

    /*
     * Set up necessary infra for making async calls to all peers at the
     * "address:port" combos given in peers.
     *
     * The order of entries in peers defines the mapping to node IDs,
     * indexed beginning from zero, and must be consistent across all
     * nodes. Operations in NetworkInterposer refer to peers by their
     * node ID.
     */
    NetworkInterposer(const std::list<Peer> &peers);

    /*
     * Containers do not release held objects, so resources must
     * be manually released by resetting the smart pointers
     */
    ~NetworkInterposer();

    /*
     * Primary functionality and raison d'etre of NetworkInterposer.
     *
     * Depending on how this is compiled, the services will talk to each
     * other as in a real application, or will talk to a central
     * NetworkInterposer service that simulates network conditions.
     */
    void broadcast(const Vote& vote, grpc::CompletionQueue* cq);

private:
    std::vector<std::shared_ptr<grpc::Channel>> channel;
    std::vector<std::unique_ptr<Streamlet::Stub>> stub;
};