#include "ThroughputLossStateMachine.h"

ThroughputLossStateMachine::ThroughputLossStateMachine(uint32_t id, uint32_t nodes, gpr_timespec print_interval)
    : ReplicatedStateMachine(), local_id{id}, num_nodes{nodes}, send_value{id}, committed{0}, lost{0}, sent{0},
      finalizations{0}, notarizations{0}, commit_time{0},
      uptime{gpr_time_0(GPR_CLOCK_MONOTONIC)},
      next_print{gpr_time_add(uptime, print_interval)},
      stats_interval{print_interval}  {

}

ThroughputLossStateMachine::~ThroughputLossStateMachine()
{

}

std::thread ThroughputLossStateMachine::SpawnThread()
{
    return std::thread{};
}

void ThroughputLossStateMachine::TransactionsFinalized(const std::string &txns, uint64_t epoch) {
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);

    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    /*
     * Finalizations occur in order and must have monotonically increasing
     * epochs. So, if a finalization is received for an epoch past the
     * head of the queue, that epoch was dropped.
     */
    while (!proposed_epochs.empty() && epoch > proposed_epochs.front()) {
        lost_epochs.push(proposed_epochs.front());
        proposed_epochs.pop();
        proposed_ts.pop();
        lost++;
    }

    /*
     * If a finalization skips to a later block proposed by this node,
     * then this if statement must be run after the loop above finishes.
     */
    if (!proposed_epochs.empty() && epoch == proposed_epochs.front()) {
        gpr_timespec diff = gpr_time_sub(ts, proposed_ts.front());
        commit_time += static_cast<double>(diff.tv_sec)
            + static_cast<double>(diff.tv_nsec) / GPR_NS_PER_SEC;
        proposed_epochs.pop();
        proposed_ts.pop();
        committed++;
    }

    finalizations++;

#ifdef DEBUG_PRINTS
    std::cout << "Node " << local_id << ": finalization for epoch " << epoch << std::endl;
        //<< " with value " << val << std::endl;
#endif
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

#ifdef BYZANTINE
void ThroughputLossStateMachine::GetTransactions(uint32_t peer, std::string *txns, uint64_t epoch) {
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);
    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    if (peer % 2 == 0) {
        *txns = "Adversary message A";
    } else {
        *txns = "Adversary message B";
    }

    txns->resize(1024); // 1 KB messages

    // Start timing from first peer
    if (peer == 0) {
        proposed_epochs.push(epoch);
        proposed_ts.push(gpr_now(GPR_CLOCK_MONOTONIC));
    }

    // Update sent only on last peer
    if (peer == num_nodes - 1) {
        sent++;
    }

    print_stats();
}

#else

void ThroughputLossStateMachine::GetTransactions(std::string *txns, uint64_t epoch) {
    gpr_timespec ts = gpr_now(GPR_CLOCK_MONOTONIC);
    uptime = gpr_time_add(uptime, gpr_time_sub(ts, prev_ts));
    prev_ts = ts;

    txns->resize(1024); // 1 KB messages

    sent++;

    proposed_epochs.push(epoch);
    proposed_ts.push(gpr_now(GPR_CLOCK_MONOTONIC));

    print_stats();
}
#endif

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
        std::cout << '\t' << (commit_time / committed) << " average seconds to commit" << std::endl;

        if (!lost_epochs.empty()) {
            std::cout << '\t' << "Lost:";
            while (!lost_epochs.empty()) {
                std::cout << ' ' << lost_epochs.front();
                lost_epochs.pop();
            }
            std::cout << std::endl;
        }
    }
}