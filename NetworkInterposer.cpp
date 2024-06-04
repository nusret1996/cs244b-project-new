#include "NetworkInterposer.h"
#include "grpcpp/create_channel.h"

NetworkInterposer::NetworkInterposer(const std::vector<Peer> &peers, uint32_t id) : local_id{id} {
    bool error = false;

    for (const Peer &remote : peers) {
        // std::cout << "setting up stub with remote " << remote.addr << std::endl;
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
        if (i == local_id) { continue; }

        // allocate and record a Pending somehwere, cleaned up when pulled from cq
        // in the service implementation
        // Pending req
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        // stub[i]->AsyncProposeBlock(&req->context, vote, cq);
        // or
        // stub[i]->AsyncNotifyVote(&req->context, vote, cq);
        pending_ptr->rpc_ptr = stub[i]->AsyncNotifyVote(&pending_ptr->context, vote, cq);
        // get a bunch of these back
        // std::unique_ptr<grpc::ClientAsyncResponseReader<Response>> rpc
        // call finish to associated with tag
        pending_ptr->rpc_ptr->Finish(&pending_ptr->resp, &pending_ptr->status, (void *) pending_ptr);
    }
}

void NetworkInterposer::broadcast(const Proposal& proposal, grpc::CompletionQueue* cq) {
    // std::cout << "networkinterposer broadcast proposal" << std::endl;
    for (size_t i = 0; i < stub.size(); i++) {
        if (i == local_id) { continue; }

        // std::cout << "network interporser broadcast i " << i << std::endl;
        // allocate and record a Pending somehwere, cleaned up when pulled from cq
        // in the service implementation
        // Pending req
        // pending_set.insert(req);
        Pending* pending_ptr = new Pending();

        // stub[i]->AsyncProposeBlock(&req->context, vote, cq);
        // or
        // stub[i]->AsyncNotifyVote(&req->context, vote, cq);
        // std::unique_ptr< ::grpc::ClientAsyncResponseReader<Response>> rpc = stub[i]->AsyncProposeBlock(&pending_ptr->context, proposal, cq);
        // std::cout << "calling async propose block" << std::endl;
        pending_ptr->rpc_ptr = stub[i]->AsyncProposeBlock(&pending_ptr->context, proposal, cq);;
        // get a bunch of these back
        // std::unique_ptr<grpc::ClientAsyncResponseReader<Response>> rpc
        // call finish to associated with tag
        pending_ptr->rpc_ptr->Finish(&pending_ptr->resp, &pending_ptr->status, (void *) pending_ptr);
    }
}
