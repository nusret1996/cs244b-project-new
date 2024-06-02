#include "CryptoManager.h"

#include <algorithm> // For string copies

#ifndef FAKE_CRYPTO
#include <stdexcept>
#include "openssl/err.h"
#endif

// Defined below
static inline std::string serialize_block(const Block &block);

static inline std::string serialize_vote(const Vote *vote);

CryptoManager::CryptoManager(const std::vector<Peer> &peers, const Key &priv, uint32_t id)
#ifdef FAKE_CRYPTO
    : pub_key{peers.at(id).key}, priv_key{priv}
#else
    : local_key{load_ed25519_private_derive_public(priv)},
    md_sha256{nullptr, EVP_MD_free},
    md_md5{nullptr, EVP_MD_free}
#endif
{
#ifdef FAKE_CRYPTO
    for (const Peer &remote : peers) {
        peer_key.emplace_back(remote.key);
    }
#else
    char error_message[13 + 256] = "EVP_MD_fetch ";

    // Get SHA256 handle
    EVP_MD *md = EVP_MD_fetch(nullptr, "SHA256", nullptr);
    if (md == nullptr) {
        ERR_error_string_n(ERR_get_error(), error_message + 13, 256);
        throw std::runtime_error{error_message};
    } else {
        md_sha256.reset(md);
    }

    // Get MD5 handle
    md = EVP_MD_fetch(nullptr, "MD5", nullptr);
    if (md == nullptr) {
        ERR_error_string_n(ERR_get_error(), error_message + 13, 256);
        throw std::runtime_error{error_message};
    } else {
        md_md5.reset(md);
    }

    // Load peers' public keys for verification
    for (const Peer &remote : peers) {
        peer_key.push_back(load_ed25519_public_only(remote.key));
    }
#endif
}

std::string CryptoManager::hash_block(const Block &block) {
    std::string buffer{serialize_block(block)};

#ifdef FAKE_CRYPTO
    std::string hash;
    hash.resize(32);
    fill(hash.begin(), hash.end(), 0);
    return hash;
#else
    char error_message[256];

    // For SHA-256
    uint8_t digest[32];
    uint32_t digest_len = 32;

    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> ctx{
        md_ctx_pool_get()
    };

    if (EVP_DigestInit_ex(ctx.get(), md_sha256.get(), nullptr) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestInit_ex " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return std::string{};
    }

    if (EVP_DigestUpdate(ctx.get(), buffer.data(), buffer.length()) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestUpdate " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return std::string{};
    }

    if (EVP_DigestFinal_ex(ctx.get(), digest, &digest_len) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestFinal_ex " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return std::string{};
    }

    return std::string{digest, digest + digest_len};
#endif
}

std::string CryptoManager::sign_block(const Block &block) {
#ifdef FAKE_CRYPTO
    // priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    std::string sig;
    sig.resize(2 * priv_key.size());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin() + priv_key.size());
    return sig;
#else

    return sign_ed25519(serialize_block(block));

#endif
}

bool CryptoManager::verify_block(uint32_t node, const std::string &sig, const Block &block) {
    if (node >= peer_key.size()) {
        return false;
    }

#ifdef FAKE_CRYPTO
    // priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    std::string k;
    const Key &pk = peer_key[node];
    k.resize(2 * pk.size());
    copy(pk.cbegin(), pk.cend(), k.begin());
    copy(pk.cbegin(), pk.cend(), k.begin() + pk.size());
    return sig == k;
#else

    return verify_ed25519(peer_key[node].get(), sig, serialize_block(block));

#endif
}

std::string CryptoManager::sign_vote(const Vote *vote) {
#ifdef FAKE_CRYPTO
    // priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    std::string sig;
    sig.resize(2 * priv_key.size());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin());
    copy(priv_key.cbegin(), priv_key.cend(), sig.begin() + priv_key.size());
    return sig;
#else

    return sign_ed25519(serialize_vote(vote));

#endif
}

bool CryptoManager::verify_vote(uint32_t node, const std::string &sig, const Vote *vote) {
    if (node >= peer_key.size()) {
        return false;
    }

#ifdef FAKE_CRYPTO
    // priv == pub and a signature is priv concatenated
    // to itself to fill out a 64 byte value
    std::string k;
    const Key &pk = peer_key[node];
    k.resize(2 * pk.size());
    copy(pk.cbegin(), pk.cend(), k.begin());
    copy(pk.cbegin(), pk.cend(), k.begin() + pk.size());
    return sig == k;
#else

    return verify_ed25519(peer_key[node].get(), sig, serialize_vote(vote));

#endif
}

uint32_t CryptoManager::hash_epoch(uint64_t epoch) {
#ifdef FAKE_CRYPTO
    // ok since the number of peers is constrained by a uint32_t
    return static_cast<uint32_t>(epoch % peer_key.size());
#else
    return static_cast<uint32_t>(epoch % peer_key.size());
#endif
}

static inline std::string serialize_block(const Block &block) {
    if (block.phash().length() != 32) {
        std::cerr << "Warning: Block phash not 256 bits in length" << std::endl;
        return std::string{};
    }

    // Reserve space for 2 64-bit ints, a 256-bit hash, and length of the transactions
    std::string buffer;
    buffer.resize(8 + 8 + 32 + block.txns().length());

    // char* is allowed to alias other types, right?
    uint64_t t = 0;
    char *v = reinterpret_cast<char*>(&t);

    // Assume that communicating machines use the same endianness, ok for demo
    t = block.parent();
    copy(v, v + 8, buffer.begin());
    t = block.epoch();
    copy(v, v + 8, buffer.begin() + 8);

    // Copy hash after the ints, even though defined in different order
    copy(block.phash().cbegin(), block.phash().cend(), buffer.begin() + 16);

    // Copy transaction data
    copy(block.txns().cbegin(), block.txns().cend(), buffer.begin() + 24);

    return buffer;
}

static inline std::string serialize_vote(const Vote *vote) {
    if (vote->hash().length() != 32) {
        std::cerr << "Warning: Vote hash not 256 bits in length" << std::endl;
        return std::string{};
    }

    // Reserve space for 2 64-bit ints and a 256-bit hash
    std::string buffer;
    buffer.resize(8 + 8 + 32);

    // char* is allowed to alias other types, right?
    uint64_t t = 0;
    char *v = reinterpret_cast<char*>(&t);

    // Assume that communicating machines use the same endianness, ok for demo
    t = vote->parent();
    copy(v, v + 8, buffer.begin());
    t = vote->epoch();
    copy(v, v + 8, buffer.begin() + 8);

    // Copy hash
    copy(vote->hash().cbegin(), vote->hash().cend(), buffer.begin() + 16);

    return buffer;
}

// Private methods only needed when building with OpenSSL
#ifndef FAKE_CRYPTO
std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> CryptoManager::md_ctx_pool_get() {
    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> t{nullptr, EVP_MD_CTX_free};
    std::unique_lock<std::mutex> p{pool_lock};

    if (!md_ctx_pool.empty()) {
        t.swap(md_ctx_pool.top());
        md_ctx_pool.pop();
        return t;
    } else {
        p.unlock();

        EVP_MD_CTX *ctx = EVP_MD_CTX_new();

        // If no allocation, print the error and return a wrapped nullptr
        if (ctx == nullptr) {
            char error_message[256];
            ERR_error_string_n(ERR_get_error(), error_message, 256);
            std::cerr << "EVP_MD_CTX_new " << error_message << std::endl;
        }

        return std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)>{
            ctx, EVP_MD_CTX_free
        };
    }
}

void CryptoManager::md_ctx_pool_put(std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> &&ctx) {
    if (EVP_MD_CTX_reset(ctx.get()) != 1) {
        char error_message[256];
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_MD_CTX_reset " << error_message << std::endl;
    } else {
        std::unique_lock<std::mutex> p{pool_lock};
        md_ctx_pool.push(std::move(ctx));
    }
}

std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> CryptoManager::load_ed25519_public_only(const Key &pubkey) {
    EVP_PKEY *pkey = EVP_PKEY_new_raw_public_key_ex(
        nullptr,
        "ED25519",
        nullptr,
        pubkey.data(),
        pubkey.size()
    );

    if (pkey == nullptr) {
        char error_message[31 + 256] = "EVP_PKEY_new_raw_public_key_ex ";
        ERR_error_string_n(ERR_get_error(), error_message + 31, 256);
        throw std::runtime_error{error_message};
    }

    return std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>{
        pkey, EVP_PKEY_free
    };
}

std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> CryptoManager::load_ed25519_private_derive_public(const Key &privkey) {
    EVP_PKEY *pkey = EVP_PKEY_new_raw_private_key_ex(
        nullptr,
        "ED25519",
        nullptr,
        privkey.data(),
        privkey.size()
    );

    if (pkey == nullptr) {
        char error_message[32 + 256] = "EVP_PKEY_new_raw_private_key_ex ";
        ERR_error_string_n(ERR_get_error(), error_message + 32, 256);
        throw std::runtime_error{error_message};
    }

    return std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>{
        pkey, EVP_PKEY_free
    };
}

std::string CryptoManager::sign_ed25519(const std::string &input) {
    char error_message[256];
    uint8_t signature[64];
    size_t sign_len = 64;
    const unsigned char *data = reinterpret_cast<const unsigned char*>(input.data());

    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> ctx{
        md_ctx_pool_get()
    };

    if (EVP_DigestSignInit_ex(ctx.get(), nullptr, nullptr, nullptr, nullptr, local_key.get(), nullptr) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestSignInit_ex " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return std::string{};
    }

    if (EVP_DigestSign(ctx.get(), signature, &sign_len, data, input.length()) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestSign " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return std::string{};
    }

    md_ctx_pool_put(std::move(ctx));

    return std::string{signature, signature + sign_len};
}

bool CryptoManager::verify_ed25519(EVP_PKEY *pkey, const std::string &signature, const std::string &input) {
    char error_message[256];

    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> ctx{
        md_ctx_pool_get()
    };

    if (EVP_DigestVerifyInit_ex(ctx.get(), nullptr, nullptr, nullptr, nullptr, pkey, nullptr) != 1) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestVerifyInit_ex " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return false;
    }

    const unsigned char *sign = reinterpret_cast<const unsigned char *>(signature.data());
    const unsigned char *data = reinterpret_cast<const unsigned char*>(input.data());

    int ret = EVP_DigestVerify(ctx.get(), sign, signature.length(), data, input.length());

    // Return value of 1 indicates the signature passed verification. Return value of 0 indicates
    // the signature did not pass verification. Other return values indicate failure in the call.
    if (!(ret == 1 || ret == 0)) {
        ERR_error_string_n(ERR_get_error(), error_message, 256);
        std::cerr << "EVP_DigestVerify " << error_message << std::endl;
        md_ctx_pool_put(std::move(ctx));
        return false;
    }

    md_ctx_pool_put(std::move(ctx));

    return ret == 1;
}
#endif