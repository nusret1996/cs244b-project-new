#include "NetworkInterposer.h"

NetworkInterposer::NetworkInterposer(const std::list<Peer> &peers) {
    bool error = false;

    for (const Peer &remote : peers) {
        channel.emplace_back(grpc::CreateChannel(remote.addr, grpc::InsecureChannelCredentials()));

        // Check returned channel isn't nullptr
        if (!channel.back()) {
            error = true;
            break;
        }

        stub.emplace_back(Streamlet::NewStub(ch));

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

        // throw exception to notify caller?
    }
}

NetworkInterposer::~NetworkInterposer() {
    // Smart pointers will call any associated deallocation routine
    for (size_t i = 0; i < stub.size(); i++) {
        stub[i].reset();
        channel[i].reset();
    }
}

void NetworkInterposer::broadcast(const Vote& vote, grpc::CompletionQueue* cq) {
    for (size_t i = 0; i < stub.size(); i++) {
        // allocate and record a Pending somehwere, cleaned up when pulled from cq
        // in the service implementation
        // Pending req

        // stub[i]->AsyncProposeBlock(&req->context, vote, cq);
        // or
        // stub[i]->AsyncNotifyVote(&req->context, vote, cq);

        // get a bunch of these back
        // std::unique_ptr< ::grpc::ClientAsyncResponseReader<Response>> rpc

        // call finish to associated with tag
        // rpc->Finish(&req->resp, &req->status, &req);
    }
}