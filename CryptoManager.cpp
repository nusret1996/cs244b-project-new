#include "CryptoManager.h"

CryptoManager::CryptoManager(const std::list<Peer> &peers, const Key &priv, const Key &pub)
    : num_peers{0} {
    // Create EVP_PKEYs for signing at local node and for verifying signatures from peers

    for (const Peer &remote : peers) {
        // allocate EVP_PKEY
        // push back EVP_PKEY* into vector

        // do something trivial for now
        num_peers++;
    }
}

CryptoManager::~CryptoManager() {

}

void CryptoManager::sha256_of(const void *data, uint64_t bytes, std::string &out) {
    // to be implemented
}

void CryptoManager::sign_sha256(const std::string &digest, std::string &out) {
    // to be implemented
}

bool CryptoManager::verify_signature(const std::string &digest, const std::string &signature) {
    // to be implemented
    return true;
}

uint32_t CryptoManager::hash_epoch(uint64_t epoch) {
    // to be implemented

    // ok for now since the number of peers is constrained by a uint32_t
    return static_cast<uint32_t>(epoch % num_peers);
}