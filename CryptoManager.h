#pragma once

#include <string>
#include <vector>

#include "structs.h"

#ifndef FAKE_CRYPTO
#include <stack>
#include <mutex>
#define OPENSSL_API_COMPAT 30000
#define OPENSSL_NO_DEPRECATED
#include "openssl/evp.h"
#endif

/*
 * Handles cryptographic operations on blocks in the
 * multithreaded environment. The latter is an issue because
 * libraries like OpenSSL require allocating and passing
 * around state/contexts for everything.
 */
class CryptoManager {
public:
    /*
     * The order of entries in peers defines the mapping to node IDs,
     * indexed beginning from zero, and must be consistent across all
     * nodes. Operations in CryptoManager refer to peers by their node ID.
     *
     * priv and pub are the keypair of the local node used to sign its
     * own messages.
     */
    CryptoManager(const std::vector<Peer> &peers, const Key &priv, uint32_t id);

    /*
     * Computes SHA-256 on binary representation of the block. Protobuf uses strings
     * for the bytes type, so the hash output is a string temporary that is intended
     * to be moved into the protobuf field.
     */
    std::string hash_block(const Block &block);

    /*
     * Computes an ED25519 signature of the binary representation of the block.
     * Same reason as in hash_block for use of strings.
     */
    std::string sign_block(const Block &block);

    /*
     * Verifies the ED25519 signature of the binary representation of the block.
     * The signature is verified using the public key of the specified node.
     */
    bool verify_block(uint32_t node, const std::string &sig, const Block &block);

    /*
     * Computes an ED25519 signature over the parent, epoch, and hash fields
     * of the vote.
     */
    std::string sign_vote(const Vote *vote);

    /*
     * Verifies and ED25519 signature over the parent, epoch, and hash fields
     * of the vote. The signature is verified using the public key of the
     * specified node.
     */
    bool verify_vote(uint32_t node, const std::string &sig, const Vote *vote);
    
    /*
     * Used to compute the leader.
     *
     * Computes an MD5 hash of the epoch represented as a big endian integer
     * and returns the value of the first 4 bytes interpreted as a big endian
     * integer mod the number of peers and returns that as the node ID.
     */
    uint32_t hash_epoch(uint64_t epoch);

private:
#ifdef FAKE_CRYPTO
    // Used for trivial development implementation
    Key pub_key;
    Key priv_key;
    std::vector<Key> peer_key;
#else
    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> md_ctx_pool_get();
    void md_ctx_pool_put(std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> &&ctx);

    std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> load_ed25519_public_only(const Key &pubkey);
    std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> load_ed25519_private_derive_public(const Key &privkey);

    std::string sign_ed25519(const std::string &input);
    bool verify_ed25519(EVP_PKEY *pkey, const std::string &signature, const std::string &input);

    // Contains both public and private key for the local node
    std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> local_key;

    // Contains only the public key for peers
    std::vector<std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>> peer_key;

    // Message digest algorithm handles
    std::unique_ptr<EVP_MD, void(*)(EVP_MD*)> md_sha256;
    std::unique_ptr<EVP_MD, void(*)(EVP_MD*)> md_md5;

    // Pool of CTX structures so threads do not interfere with each
    // other's crypto operations. Since the threading model underlying
    // the gRPC service implementation isn't known, this allows us
    // to expand the number of contexts on demand. Objects in the pool
    // are ready for use in the new/reset state, and must be reset
    // by the user before being placed back in the pool.
    std::mutex pool_lock;
    std::stack<std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)>> md_ctx_pool;
#endif
};