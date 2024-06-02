#include "utils.h"

#include <fstream>

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

int main(int argc, const char *argv[]) {
    if (argc <= 2) {
        std::cout << "Usage: ./make_peerconfig <output file> <host:port> [<host:port> ...]" << std::endl;
        return 1;
    }

    std::ofstream out_file{argv[1]};

    if (!out_file) {
        std::cerr << "Unable to open " << argv[1] << " as output file" << std::endl;
        return 1;
    }

    std::string pub_hex;
    std::string priv_hex;

    for (int i = 2; i < argc && out_file; i++) {
        auto pkey = gen_ed25519_pair();

        std::pair<Key, Key> p = key_bytes(pkey.get(), false);

        write_hexstring(pub_hex, p.first.data(), p.first.size());
        write_hexstring(priv_hex, p.second.data(), p.second.size());

        out_file << argv[i] << ' ' << pub_hex << ' ' << priv_hex << std::endl;
    }

    if (!out_file) {
        std::cerr << "Error writing to " << argv[1] << std::endl;
        return 1;
    }

    return 0;
}