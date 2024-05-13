#include "CryptoManager.h"
#include <algorithm>

CryptoManager::CryptoManager(const std::vector<Peer> &peers, const Key &priv, const Key &pub)
    : pub_key{pub}, priv_key{priv} {

    // create EVP_PKEY containing local node's private and public keys for signing

    for (const Peer &remote : peers) {
        // create EVP_PKEY with only public key and push the pointer into vector

        // do something trivial for now
        peer_key.emplace_back(remote.key);
    }
}

CryptoManager::~CryptoManager() {

}

void CryptoManager::hash_block(const Block *block, std::string &hash) {
    // Copy fields individiually if the serialization turns out to be
    // nondeterminstic. Should be determinisitic for the same version of
    // the library and language binding.
    std::string buffer;
    block->SerializeToString(&buffer);
    return sha256_of(buffer.data(), buffer.length(), hash);
}

void CryptoManager::sha256_of(const void *data, uint64_t bytes, std::string &hash) {
    // to be implemented
}

void CryptoManager::sign_sha256(const std::string &digest, std::string &sig) {
    // to be implemented

    // for now priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    sig.resize(2 * priv_key.size());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin() + priv_key.size());
}

bool CryptoManager::verify_signature(uint32_t node, const std::string &digest, const std::string &sig) {
    // to be implemented
    if (node >= peer_key.size()) {
        return false;
    }

    // for now priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    std::string k;
    const Key &pk = peer_key[node];
    k.resize(2 * pk.size());
    copy(pk.cbegin(), pk.cend(), k.begin());
    copy(pk.cbegin(), pk.cend(), k.begin() + pk.size());
    return sig == k;
}

uint32_t CryptoManager::hash_epoch(uint64_t epoch) {
    // to be implemented

    // ok for now since the number of peers is constrained by a uint32_t
    return static_cast<uint32_t>(epoch % peer_key.size());
}