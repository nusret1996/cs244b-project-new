#pragma once

#include "grpc/support/time.h"
#include "structs.h"
#include <queue>

class ThroughputLossStateMachine : public ReplicatedStateMachine {
public:
    ThroughputLossStateMachine(uint32_t id, uint32_t nodes, gpr_timespec print_interval);
    void TransactionsFinalized(const std::string &txns, uint64_t epoch) override;
    void TransactionsNotarized(const std::string &txns, uint64_t epoch) override;
    bool ValidateTransactions(const std::string &txns, uint64_t epoch) override;
    void GetTransactions(std::string *txns, uint64_t epoch) override;
    void BeginTime() override;

private:
    void print_stats();

    const uint32_t local_id;
    const uint32_t num_nodes;

    uint64_t send_value;

    // Number of proposals from the local node that have been finalized
    uint64_t committed;

    // Number of proposals from the local node that are known to have
    // been excluded in the finalized chain
    uint64_t lost;

    // Number of proposals sent from the local node. Assuming all outstanding
    // RPCs have finished processing, sent - (comitted + lost) gives the total
    // number of proposals lost in the sense that there were insufficient votes
    // to notarize the block.
    uint64_t sent;

    // Finalizations seen for any block to measure system throughput
    uint64_t finalizations;

    // Notarizations seen for any block to measure system throughput
    uint64_t notarizations;
    
    // Total time, in nanoseconds, from proposal to commit. Used to compute average
    // time until commit and only includes blocks that have been committed.
    double commit_time;

    // Total time since start. Used to compute average finalizations
    // and notarizations per second.
    gpr_timespec uptime;

    // Next value of uptime at which to print statistics
    gpr_timespec next_print;

    // Interval at which to print statistics
    gpr_timespec stats_interval;

    // Previous timestamp from which to calculate elapsed time
    gpr_timespec prev_ts;

    // Epochs in which this node has propsed a block
    std::queue<uint64_t> proposed_epochs;

    // Timestamp for proposals, allowing us to track time until notarization
    std::queue<gpr_timespec> proposed_ts;
};