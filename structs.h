#pragma once

#include "streamlet.pb.h"

// Interface for StreamletNodes to talk to applications
// which implement the replicated state machine
class ReplicatedStateMachine {
public:
    virtual ~ReplicatedStateMachine() = 0;
    virtual void TransactionsFinalized(/* Binary transaction blob */) = 0;
    virtual void TransactionsNotarized(/* Blocks */) = 0;
    virtual bool ValidateTransaction(/* Binary transaction blob */);
};

// 256 bit key for P-256/secp256r1
using Key = std::array<uint8_t, 32>;

/*
 * Used to initialze the streamlet service.
 */
struct Peer {
    std::string addr;
    Key key;
};

/*
 * A block on the blockchain containing the protobuf Block type
 * alongside information needed to implement the Streamlet protocol.
 * 
 * From the protocol descritpion, a block is a triple of the parent hash,
 * epoch number, and transaction data for the replicated state machine.
 * ChainElements are intended to be a unique references to elements of
 * the blockchain, i.e. there must only be one ChainElement in memory
 * representing a given triple although there can be several Block
 * instances representing the same mathematical object.
 *
 * A ChainElement is constructed when the node receives notice of a newly
 * proposed block once it has verified the proposal is legitimate.
 * The ChainElement is used to cache the propsed block and record votes,
 * and once notarized, to add the block to the blockchain.
 */
struct ChainElement {
    // Structure synchronization
    std::mutex m;

    // Flag to coordinate structure deletion with ref_count
    bool removed;

    // Reference count to coordinate structure deletion.
    // Expect fewer than 256 threads.
    std::atomic_uint8_t ref_count;

    // Since votes from other nodes can arrive before the leader proposal
    // in an epoch, this records the hash that each node voted on. Later,
    // once the leader's proposal is seen and verified, only the votes
    // with a matching hash are recorded in voters below, and this structure
    // is cleared.
    std::unordered_map<uint32_t, std::string> vote_hashes;

    // std::vector<bool> is specialized on major platforms to be
    // a compact bitvector. std::bitset cannot be used since we do
    // not know the system configuration ahead of time (bitset size
    // is a template parameter).
    std::vector<bool> voters;

    // Number of unique votes seen for this block. Equal to
    // the number of true entries in the vector above.
    uint32_t votes;

    // The block hash
    std::string hash;

    // The block
    Block block;

    // Index of this block in its chain
    uint64_t index;

    ChainElement(const uint32_t num_peers)
        : removed{false}, ref_count{0}, votes{0}, hash{}, block{}, index{0} {
            voters.resize(num_peers);
    }
};