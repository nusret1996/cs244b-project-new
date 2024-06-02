#include <memory>
#include <stdexcept>

#include "utils.h"

#define OPENSSL_API_COMPAT 30000
#define OPENSSL_NO_DEPRECATED
#include "openssl/evp.h"
#include "openssl/err.h"

// From structs.h: 256 bit key for ED25519
using Key = std::array<uint8_t, 32>;

// Add underscore to differentiate from standard library assert
static char openssl_str_buffer[256];
#define _assert(cond, errmsg) if (!(cond)) throw std::runtime_error{(errmsg)}
#define _openssl(cond) if (!(cond)) { \
    ERR_error_string_n(ERR_get_error(), openssl_str_buffer, 256); \
    throw std::runtime_error{openssl_str_buffer}; \
}

std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> gen_ed25519_pair() {
    EVP_PKEY *pkey = nullptr;

    EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_from_name(nullptr, "ED25519", nullptr);
    _openssl(ctx != nullptr);

    // Wrap here to ensure ctx is deallocated if exit by exception
    std::unique_ptr<EVP_PKEY_CTX, void(*)(EVP_PKEY_CTX*)> ctx_wrapper{
        ctx, EVP_PKEY_CTX_free
    };

    _openssl(EVP_PKEY_keygen_init(ctx) == 1);

    _openssl(EVP_PKEY_keygen(ctx, &pkey) == 1);

    return std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>{
        pkey, EVP_PKEY_free
    };
}

std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> load_ed25519_public_only(Key &pubkey) {
    EVP_PKEY *pkey = EVP_PKEY_new_raw_public_key_ex(
        nullptr,
        "ED25519",
        nullptr,
        pubkey.data(),
        pubkey.size()
    );

    _openssl(pkey != nullptr);

    return std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>{
        pkey, EVP_PKEY_free
    };
}

std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)> load_ed25519_private_derive_public(Key &privkey) {
    EVP_PKEY *pkey = EVP_PKEY_new_raw_private_key_ex(
        nullptr,
        "ED25519",
        nullptr,
        privkey.data(),
        privkey.size()
    );

    _openssl(pkey != nullptr);

    return std::unique_ptr<EVP_PKEY, void(*)(EVP_PKEY*)>{
        pkey, EVP_PKEY_free
    };
}

std::string sign_ed25519(EVP_MD_CTX *ctx, EVP_PKEY *pkey, const std::string &input) {
    // Turns out the HashEdDSA schemes don't have functionality yet anyway,
    // so the digest cannot be separated from the signature with this either
    // static char param_val[] = "Ed25519ph";
    // const OSSL_PARAM params[] = {
    //     OSSL_PARAM_utf8_string("instance", &param_val[0], 9),
    //     OSSL_PARAM_END
    // };
 
    uint8_t signature[64];
    size_t sign_len = 64;
    const unsigned char *data = reinterpret_cast<const unsigned char*>(input.data());

    _openssl(EVP_MD_CTX_reset(ctx) == 1);

    _openssl(EVP_DigestSignInit_ex(ctx, nullptr, nullptr, nullptr, nullptr, pkey, nullptr) == 1);

    _openssl(EVP_DigestSign(ctx, signature, &sign_len, data, input.length()) == 1);

    _openssl(EVP_MD_CTX_reset(ctx) == 1);

    return std::string{signature, signature + sign_len};
}

bool verify_ed25519(EVP_MD_CTX *ctx, EVP_PKEY *pkey, const std::string &signature, const std::string &input) {
    _openssl(EVP_DigestVerifyInit_ex(ctx, nullptr, nullptr, nullptr, nullptr, pkey, nullptr) == 1);

    const unsigned char *sign = reinterpret_cast<const unsigned char *>(signature.data());
    const unsigned char *data = reinterpret_cast<const unsigned char*>(input.data());

    int ret = EVP_DigestVerify(ctx, sign, signature.length(), data, input.length());

    // Return value of 1 indicates the signature passed verification. Return value of 0 indicates
    // the signature did not pass verification. Other return values indicate failure in the call.
    _openssl(ret == 1 || ret == 0);

    _openssl(EVP_MD_CTX_reset(ctx) == 1);

    return ret == 1;
}

std::pair<Key, Key> key_bytes(EVP_PKEY *pkey, bool public_only) {
    Key pub;
    Key priv;

    size_t pub_len = pub.size();
    _openssl(EVP_PKEY_get_raw_public_key(pkey, pub.data(), &pub_len) == 1);
    _assert(pub_len == 32, "pub_len expected to be 32 after writing key");

    if (!public_only) {
        size_t priv_len = priv.size();
        _openssl(EVP_PKEY_get_raw_private_key(pkey, priv.data(), &priv_len) == 1);
        _assert(priv_len == 32, "priv_len expected to be 32 after writing key");
    }

    return std::make_pair(pub, priv);
}

std::unique_ptr<EVP_MD, void(*)(EVP_MD*)> get_md_sha512() {
    EVP_MD *md = EVP_MD_fetch(nullptr, "SHA512", nullptr);
    _openssl(md != nullptr);

    return std::unique_ptr<EVP_MD, void(*)(EVP_MD*)>{
        md, EVP_MD_free
    };
}

std::unique_ptr<EVP_MD, void(*)(EVP_MD*)> get_md_sha256() {
    EVP_MD *md = EVP_MD_fetch(nullptr, "SHA256", nullptr);
    _openssl(md != nullptr);

    return std::unique_ptr<EVP_MD, void(*)(EVP_MD*)>{
        md, EVP_MD_free
    };
}

std::unique_ptr<EVP_MD, void(*)(EVP_MD*)> get_md_md5() {
    EVP_MD *md = EVP_MD_fetch(nullptr, "MD5", nullptr);
    _openssl(md != nullptr);

    return std::unique_ptr<EVP_MD, void(*)(EVP_MD*)>{
        md, EVP_MD_free
    };
}

std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> make_md_ctx() {
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    _openssl(ctx != nullptr);

    return std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)>{
        ctx, EVP_MD_CTX_free
    };
}

std::string get_hash(const std::string &val, const EVP_MD *md) {
    // Ensures ctx is deallocated if exit by exception
    std::unique_ptr<EVP_MD_CTX, void(*)(EVP_MD_CTX*)> ctx{make_md_ctx()};

    // Up to 512 bits for SHA-512
    uint8_t digest[64];
    uint32_t digest_len = 64;

    _openssl(EVP_DigestInit_ex(ctx.get(), md, nullptr) == 1);

    _openssl(EVP_DigestUpdate(ctx.get(), val.data(), val.length()) == 1);

    _openssl(EVP_DigestFinal_ex(ctx.get(), digest, &digest_len) == 1);

    // If EVP_DigestFinal_ex were to fail, would need to reset ctx

    return std::string{digest, digest + digest_len};
}

int main(int argc, const char *argv[]) {
    if (argc != 2) {
        std::cout << "Expected data on command line" << std::endl;
        return 1;
    }

    std::string data{argv[1]};

    auto pkey = gen_ed25519_pair();

    std::pair<Key, Key> p = key_bytes(pkey.get(), false);

    std::string pubkey_hex;

    std::cout << "======== Random Keys ========" << std::endl;
    std::string hex;
    write_hexstring(hex, p.first.data(), p.first.size());
    std::cout << "Public key: " << hex << std::endl;
    pubkey_hex = hex; // copy to test loading key and verifying signature below
    write_hexstring(hex, p.second.data(), p.second.size());
    std::cout << "Private key: " << hex << std::endl;
    hex.clear();
    std::cout << std::endl;

    std::cout << "======== Hash Values ========" << std::endl;
    auto sha512 = get_md_sha512();
    auto sha256 = get_md_sha256();
    auto md5 = get_md_md5();
    std::string hash = get_hash(data, sha512.get());
    write_hexstring(hex, hash);
    std::cout << "SHA-512: " << hex << std::endl;
    hash = get_hash(data, sha256.get());
    write_hexstring(hex, hash);
    std::cout << "SHA-256: " << hex << std::endl;
    hash = get_hash(data, md5.get());
    write_hexstring(hex, hash);
    std::cout << "MD5: " << hex << std::endl;
    hex.clear();
    std::cout << std::endl;

    std::cout << "======== Signature ========" << std::endl;
    auto mdctx = make_md_ctx();
    const std::string sig{sign_ed25519(mdctx.get(), pkey.get(), data)};
    write_hexstring(hex, sig);
    std::cout << "Signature: " << hex << std::endl;
    hex.clear();
    std::cout << std::endl;

    std::cout << "======== Verification ========" << std::endl;
    Key verify_key;
    // Test reading the key back from a formatted hex string
    read_hexstring(verify_key.data(), verify_key.size(), pubkey_hex);
    // And load only the public key for signature verification
    pkey = load_ed25519_public_only(verify_key);

    // Extract public key
    p = key_bytes(pkey.get(), true);
    write_hexstring(hex, p.first.data(), p.first.size());
    std::cout << "Read back key: " << hex << std::endl;
    hex.clear();

    bool verified = verify_ed25519(mdctx.get(), pkey.get(), sig, data);
    if (verified)
        std::cout << "Signature: OK" << std::endl;
    else
        std::cout << "Signature: BAD" << std::endl;
    std::cout << std::endl;

    std::cout << "======== Signature Negative Test ========" << std::endl;
    std::string corrupted{sig};
    corrupted[0] = ~corrupted[0];
    corrupted[63] = ~corrupted[63];

    verified = verify_ed25519(mdctx.get(), pkey.get(), corrupted, data);
    if (!verified)
        std::cout << "Detected corruption: OK" << std::endl;
    else
        std::cout << "Detected corruption: MISSED" << std::endl;
    std::cout << std::endl;

    std::cout << "======== Key Negative Test ========" << std::endl;
    auto wrongkey = gen_ed25519_pair();
    std::pair<Key, Key> wrong = key_bytes(wrongkey.get(), false);
    write_hexstring(hex, wrong.first.data(), wrong.first.size());
    std::cout << "Wrong public key: " << hex << std::endl;
    write_hexstring(hex, wrong.second.data(), wrong.second.size());
    std::cout << "Wrong private key: " << hex << std::endl;
    hex.clear();

    const std::string wrongsig{sign_ed25519(mdctx.get(), wrongkey.get(), data)};
    write_hexstring(hex, wrongsig);
    std::cout << "Wrong signature: " << hex << std::endl;
    hex.clear();

    verified = verify_ed25519(mdctx.get(), pkey.get(), wrongsig, data);
    if (!verified)
        std::cout << "Bad signature: OK" << std::endl;
    else
        std::cout << "Bad signature: MISSED" << std::endl;
    std::cout << std::endl;

    return 0;
}