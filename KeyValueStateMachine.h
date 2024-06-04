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
    void BeginTime() override;
    public:
         KeyValueStateMachine(uint32_t);
        ~KeyValueStateMachine() override;
        void TransactionsFinalized(std::string) override;
        void TransactionsNotarized(std::string) override;
        bool ValidateTransaction(std::string) override;
        std::string GetTransactions() override;
        std::thread SpawnThread() override;
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
        void InputThread();

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
