#include "streamlet.pb.h"

// Interface for StreamletNodes to talk to applications
// which implement the replicated state machine
class ReplicatedStateMachine {
public:
    virtual ~ReplicatedStateMachine() = 0;
    virtual void TransactionsFinalized(/* Representation of transactions */) = 0;
    virtual void TransactionsNotarized(/* Blocks */) = 0;
};

// 256 bit key for P-256/secp256r1
using Key = std::array<uint8_t, 32>;

/*
 * Used to initialze the streamlet service.
 */
struct Peer {
    std::string addr;
    Key key;
}

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
    ChainElement *parent;
    uint64_t index;

    // std::vector<bool> is specialized on major platforms
    // to be a compact bitvector. std::bitset cannot be used
    // since we do not know the system configuration ahead
    // of time (bitset size is a template parameter).
    std::vector<bool> votes;

    // The block triple
    Block block;
}