#include "ThroughputLossStateMachine.h"

ThroughputLossStateMachine::ThroughputLossStateMachine(uint32_t id, uint32_t nodes, gpr_timespec print_interval)
    : local_id{id}, num_nodes{nodes}, send_value{id}, committed{0}, lost{0}, sent{0},
      finalizations{0}, notarizations{0}, commit_time{0},
      uptime{gpr_time_0(GPR_CLOCK_MONOTONIC)},
      next_print{gpr_time_add(uptime, print_interval)},
      stats_interval{print_interval} {

}

void ThroughputLossStateMachine::TransactionsFinalized(const std::string &txns, uint64_t epoch) {
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);

    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    /*uint64_t val = 0;
    uint8_t byte;

    for (unsigned i = 0; i < 8; i++) {
        byte = txns[i];

        val <<= 8;
        val |= byte;
    }*/

    /*
     * Finalizations occur in order and must have monotonically increasing
     * epochs. So, if a finalization is received for an epoch past the
     * head of the queue, that epoch was dropped.
     */
    while (epoch > proposed_epochs.front()) {
        proposed_epochs.pop();
        proposed_ts.pop();
        lost++;
    }

    /*
     * If a finalization skips to a later block proposed by this node,
     * then this if statement must be run after the loop above finishes.
     */
    if (epoch == proposed_epochs.front()) {
        gpr_timespec diff = gpr_time_sub(ts, proposed_ts.front());
        commit_time += static_cast<double>(diff.tv_sec)
            + static_cast<double>(diff.tv_nsec) / GPR_NS_PER_SEC;
        proposed_epochs.pop();
        proposed_ts.pop();
        committed++;
    }

    finalizations++;

#ifdef PRINT_TRANSACTIONS
    std::cout << "Node " << local_id << ": finalization for epoch " << epoch << std::endl;
#endif
    //    << " with value " << val << std::endl;
}

void ThroughputLossStateMachine::TransactionsNotarized(const std::string &txns, uint64_t epoch) {
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);

    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    notarizations++;
}

bool ThroughputLossStateMachine::ValidateTransactions(const std::string &txns, uint64_t epoch) {
    return true;
}

void ThroughputLossStateMachine::GetTransactions(std::string *txns, uint64_t epoch) {
    // uint8_t byte;
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);
    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    txns->resize(1024); // 1 KB messages

    /*for (unsigned i = 0; i < 8; i++) {
        byte = (send_counter >> (8 * (7 - i))) & 0xff;
        (*txns)[i] = byte;
    }

    send_value += num_nodes;*/

    sent++;

    proposed_epochs.push(epoch);
    proposed_ts.push(gpr_now(GPR_CLOCK_MONOTONIC));

    print_stats();
}

void ThroughputLossStateMachine::BeginTime() {
    prev_ts = gpr_now(GPR_CLOCK_MONOTONIC);
}

void ThroughputLossStateMachine::print_stats() {
    if (gpr_time_cmp(uptime, next_print) >= 0) {
        next_print = gpr_time_add(next_print, stats_interval);

        double total_sec = static_cast<double>(uptime.tv_sec)
            + static_cast<double>(uptime.tv_nsec) / GPR_NS_PER_SEC;
        
        std::cout << "Node " << local_id << ": " << lost << " lost, "
            << committed << " committed, " << (sent - (committed + lost)) << " unaccounted for" << std::endl;
        std::cout << '\t' << (finalizations / total_sec) << " finalizations per second" << std::endl;
        std::cout << '\t' << (notarizations / total_sec) << " notarizations per second" << std::endl;
        std::cout << '\t' << (committed / commit_time) << " average seconds to commit" << std::endl;
    }
}