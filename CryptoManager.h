#include <string>
#include <vector>
#include <stack>

#include "structs.h"

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
    CryptoManager(const std::list<Peer> &peers, const Key &priv, const Key &pub);

    /*
     * OpenSSL has a ton of state to clean up.
     */
    ~CryptoManager();

    /*
     * Protobuf uses strings for the byte type, so this allows
     * the field to be passed directly so that storage space
     * can be checked and accessed directly
     */
    void sha256_of(const void *data, uint64_t bytes, std::string &out);

    /*
     * Computes the P-256 signature and places. Same reason as in
     * sha256_of for use of strings.
     */
    void sign_sha256(const std::string &digest, std::string &out);

    /*
     * Computes the P-256 signature and places. Same reason as in
     * sha256_of for use of strings.
     */
    bool verify_signature(const std::string &digest, const std::string &signature);
    
    /*
     * Used to compute the leader.
     *
     * Computes an MD5 hash of the epoch represented as a big endian integer
     * and returns the value of the first 8 bytes interpreted as a big endian
     * integer mod the number of peers and returns that as the node ID.
     */
    uint32_t hash_epoch(uint64_t epoch);

private:
    // If we use OpenSSL functionality later
    // EVP_PKEY with both private and public key for local node
    // EVP_PKEY with only public key associated per remote node
    // EVP_MD instance for the class
    // EVP_PKEY_CTX per thread, or per request
    // EVP_MD_CTX per thread, or per request

    uint32_t num_peers; // For trivial implementation

    // EVP_PKEY *pub_key;
    // EVP_PKEY *priv_key;
    // std::vector<EVP_PKEY*> peer_key;

    // Pool of CTX structures so threads do not interfere with each
    // other's crypto operations. Since the threading model underlying
    // the gRPC service implementation isn't known, this allows us
    // to expand the number of contexts on demand. Objects in the pool
    // are ready for use in the new/reset state, and must be reset
    // by the user before being placed back in the pool.
    // std::mutex pool_lock;
    // std::stack<EVP_PKEY_CTX*> pkey_ctx_pool;
    // std::stack<EVP_MD_CTX*> md_ctx_pool;
};