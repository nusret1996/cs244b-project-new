#pragma once

#include "structs.h"
#include <queue>

class KeyValueStateMachine : public ReplicatedStateMachine {
public:
    KeyValueStateMachine(uint32_t id);
    void TransactionsFinalized(const std::string &txns, uint64_t epoch) override;
    void TransactionsNotarized(const std::string &txns, uint64_t epoch) override;
    bool ValidateTransactions(const std::string &txns, uint64_t epoch) override;
    void GetTransactions(std::string *txns, uint64_t epoch) override;

private:
    struct Status{
        bool onchain = false;
        bool notarizied = false;
        bool finalized = false;
        int value = -1;
    };
    std::unordered_map<int, Status> states;
    std::queue<std::pair<int, int>> to_add;
    std::pair<int,int> parse_string(std::string);
};
