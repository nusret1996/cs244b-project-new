#include "NetworkInterposer.h"
#include "grpcpp/create_channel.h"
#include "grpc++/grpc++.h"

#include <stdexcept>

NetworkInterposer::NetworkInterposer(const std::vector<Peer> &peers, uint32_t id) : local_id{id} {
    bool error = false;

    for (const Peer &remote : peers) {
        channel.emplace_back(grpc::CreateChannel(remote.addr, grpc::InsecureChannelCredentials()));

        // Check returned channel isn't nullptr
        if (!channel.back()) {
            error = true;
            break;
        }

        stub.emplace_back(Streamlet::NewStub(channel.back()));

        // Check returned stub isn't nullptr
        if (!stub.back()) {
            error = true;
            break;
        }
    }

    // Smart pointers will deallocate as needed and
    // do nothing if nullptr is held
    if (error) {
        for (size_t i = 0; i < stub.size(); i++) {
            stub[i].reset();
        }

        for (size_t i = 0; i < channel.size(); i++) {
            channel[i].reset();
        }

        throw std::runtime_error{"Could not set up client side stub and channels"};
    }
}

NetworkInterposer::~NetworkInterposer() {
    // Smart pointers will call any associated deallocation routine
    for (size_t i = 0; i < stub.size(); i++) {
        stub[i].reset();
        channel[i].reset();
    }
}

/*
 * These two broadcast functions allocate memory which is intended to be deallocated
 * in the GRPC service implementation. For simplicity, we aren't tracking how many
 * requests we've made or the associated memory. See the comment in the service Run().
 */
void NetworkInterposer::broadcast(const Vote& vote, grpc::CompletionQueue* cq) {
    for (size_t i = 0; i < stub.size(); i++) {
        if (i == local_id) { continue; }

        // allocate and record a Pending somehwere, cleaned up when pulled from cq
        // in the service implementation
        // Pending req
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        pending_ptr->rpc_ptr = stub[i]->AsyncNotifyVote(&pending_ptr->context, vote, cq);

        pending_ptr->rpc_ptr->Finish(&pending_ptr->resp, &pending_ptr->status, pending_ptr);
    }
}

void NetworkInterposer::broadcast(const Proposal& proposal, grpc::CompletionQueue* cq) {
    for (size_t i = 0; i < stub.size(); i++) {
        if (i == local_id) { continue; }

        // Pending req
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        pending_ptr->rpc_ptr = stub[i]->AsyncProposeBlock(&pending_ptr->context, proposal, cq);;

        pending_ptr->rpc_ptr->Finish(&pending_ptr->resp, &pending_ptr->status, pending_ptr);
    }
}

#ifdef BYZANTINE
void NetworkInterposer::send_single(uint32_t to, const Proposal& proposal, grpc::CompletionQueue* cq) {
    Pending* pending_ptr = new Pending();

    pending_ptr->rpc_ptr = stub[to]->AsyncProposeBlock(&pending_ptr->context, proposal, cq);;

    pending_ptr->rpc_ptr->Finish(&pending_ptr->resp, &pending_ptr->status, pending_ptr);
}
#endif