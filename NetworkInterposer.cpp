#include "NetworkInterposer.h"
#include "grpcpp/create_channel.h"

NetworkInterposer::NetworkInterposer(const std::vector<Peer> &peers) {
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
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        // // stub[i]->AsyncProposeBlock(&req->context, vote, cq);
        // // or
        // // stub[i]->AsyncNotifyVote(&req->context, vote, cq);
        std::unique_ptr< ::grpc::ClientAsyncResponseReader<Response>> rpc = stub[i]->AsyncNotifyVote(&pending_ptr->context, vote, cq);
        // get a bunch of these back
        // std::unique_ptr<grpc::ClientAsyncResponseReader<Response>> rpc
        // call finish to associated with tag
        rpc->Finish(&pending_ptr->resp, &pending_ptr->status, (void *) pending_ptr);
    }
}

void NetworkInterposer::broadcast(const Proposal& proposal, grpc::CompletionQueue* cq) {
    for (size_t i = 0; i < stub.size(); i++) {
        // allocate and record a Pending somehwere, cleaned up when pulled from cq
        // in the service implementation
        // Pending req
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        // // stub[i]->AsyncProposeBlock(&req->context, vote, cq);
        // // or
        // // stub[i]->AsyncNotifyVote(&req->context, vote, cq);
        std::unique_ptr< ::grpc::ClientAsyncResponseReader<Response>> rpc = stub[i]->AsyncProposeBlock(&pending_ptr->context, proposal, cq);
        // get a bunch of these back
        // std::unique_ptr<grpc::ClientAsyncResponseReader<Response>> rpc
        // call finish to associated with tag
        rpc->Finish(&pending_ptr->resp, &pending_ptr->status, (void *) pending_ptr);
    }
}
