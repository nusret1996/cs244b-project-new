#include "structs.h"
#include "utils.h"
#include <queue>
#include <iostream>
class KeyValueStateMachine : public ReplicatedStateMachine {
    public:
         KeyValueStateMachine(uint32_t);
        ~KeyValueStateMachine() override;
        void TransactionsFinalized(std::string) override;
        void TransactionsNotarized(std::string) override;
        bool ValidateTransaction(std::string) override;
        std::string GetTransactions() override;

    private:
        struct Status{
            bool onchain = false;
            bool notarizied = false;
            bool finalized = false;
            int value = -1;
        };
        std::unordered_map<int, Status> master;
        std::queue<std::pair<int, int>> to_add;
        std::pair<int,int> parse_string(std::string);
        

};
