#include "structs.h"
#include "utils.h"
#include <queue>
#include <iostream>
#include "KeyValueStateMachine.h"
//TODO: add protection for stoi/substr and also running out of key/value pairs
KeyValueStateMachine::KeyValueStateMachine(uint32_t id) :
    ReplicatedStateMachine() {
    for (int i = id; i < 100; i += 2){
        master.insert({i, Status({false, false, false, i + 100})});
        to_add.push({i, i + 100});
    }
}
KeyValueStateMachine::~KeyValueStateMachine()
{

}
void KeyValueStateMachine::TransactionsFinalized(std::string transaction)
{

    std::cout << "finalized: " << transaction << std::endl;
    std::pair<int, int> t = parse_string(transaction);
    if (master.find(t.first) == master.end()) {
        master.insert({t.first, Status({true, true, true, t.second})});
    } else {
        master[t.first].finalized = true;
    }
}
void KeyValueStateMachine::TransactionsNotarized(std::string transaction)
{
    std::cout << "notarized: " << transaction << std::endl;
    std::pair<int, int> t = parse_string(transaction);
    if (master.find(t.first) == master.end()) {
        master.insert({t.first, Status({true, true, false, t.second})});
    } else {
        master[t.first].notarizied = true;
    }
}
bool KeyValueStateMachine::ValidateTransaction(std::string transaction)
{
    return true;
}
std::string KeyValueStateMachine::GetTransactions()
{
    if (to_add.empty()) return "";
    std::pair<int, int> n = to_add.front();
    to_add.pop();
    std::string ret = std::string("key: " + std::to_string(n.first) + " value: " + std::to_string(n.second));
    std::cout << "get transactions: " << ret << std::endl;
    master[n.first].onchain = true;
    return ret;
}

std::pair<int, int> KeyValueStateMachine::parse_string(std::string s)
{
    if (s.length() < 13 || !s.find(" value: ")) return std::make_pair(-1, -1);
    int k = stoi(s.substr(5, s.find(" value: ")));
    int v = stoi(s.substr(s.find(" value: ") + 8));

    return std::make_pair(k, v);
}
