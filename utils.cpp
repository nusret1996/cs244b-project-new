#include "utils.h"

#include <iomanip>
#include <fstream>
#include <sstream>
#include <cstring>
#include <chrono>

#include "grpcpp/support/time.h"

int sync_time(const char *utc_start, gpr_timespec &tspec) {
    static_assert(std::chrono::system_clock::period::num == 1
        && std::chrono::system_clock::period::den >= 1000,
        "std::chrono::system_clock precision must be at least a millisecond");

    std::tm t_sync;
    char c;
    std::istringstream reader{utc_start};

    reader >> std::get_time(&t_sync, "%H:%M:%S");
    if (!reader) {
        return 1;
    }

    // There should be no additional characters following the time
    reader >> c;
    if (reader) {
        return 1;
    }

    // std::cout << t_sync.tm_hour << ':' << t_sync.tm_min << ':' << t_sync.tm_sec << std::endl;

    // Seconds into today at which to sync
    int sync_sec = t_sync.tm_hour * 3600 + t_sync.tm_min * 60 + t_sync.tm_sec;

    std::time_t t_now;
    ::time(&t_now);
    const std::tm *tm = ::gmtime(&t_now);
    int now_sec = tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;

    // Start time must be in the future
    if (sync_sec <= now_sec) {
        return 2;
    }

    grpc::TimePoint<std::chrono::system_clock::time_point> tpoint{
        std::chrono::system_clock::from_time_t(t_now) + std::chrono::seconds(sync_sec - now_sec)
    };

    tspec = gpr_convert_clock_type(tpoint.raw_time(), GPR_CLOCK_MONOTONIC);

    return 0;
}

int load_config(const char *file, uint32_t id, std::vector<Peer> &peers, Key &priv) {
    std::ifstream f{file};
    char *line = new char[1024];
    int status = 0;
    uint32_t count = 0;
    char *space;
    char *prev;

    if (!f.is_open()) {
        return 1;
    }

    while (f.getline(line, 1024)) {
        Peer p;

        // Read blank line
        if (f.gcount() == 1) {
            break;
        }

        // Extract address
        space = strchr(line, ' ');

        if (!space) {
            status = 2;
            break;
        }

        p.addr = std::string{line, space};

        // Extract public key
        do {
            prev = space + 1;
            space = strchr(prev, ' ');
        } while (space && space == prev);

        if (!space || space - prev != 64) {
            status = 2;
            break;
        }

        // key begins at prev which is one past character the last space
        read_hexstring(p.key.data(), 32, prev);

        // Private key, make sure not malformed regardless of whether
        // the key is extracted
        do {
            prev = space + 1;
            space = strchr(prev, ' ');
        } while (space && space == prev);

        if (strlen(prev) != 64) {
            status = 2;
            break;
        }

        // Copy the key
        if (count == id) {
            read_hexstring(priv.data(), 32, prev);
        }

        peers.emplace_back(p);

        count++;
    }

    if (f.fail() && !f.eof()) {
        status = 3;
    }

    delete[] line;

    return status;
}

std::string make_loopback(const std::string &local_host) {
    size_t split = local_host.find(':');

    std::string loopback{local_host};
    loopback.erase(0, split);
    loopback.insert(0, "0.0.0.0");

    return loopback;
}

size_t read_hexstring(uint8_t *bytes, size_t len, const std::string &str) {
    char b[3] = { '\0', '\0', '\0' };
    size_t pos = 0;

    while (pos < len && (2 * pos + 1) < str.length()) {
        b[0] = str[2 * pos];
        b[1] = str[2 * pos + 1];
        if (std::isxdigit(b[0]) && std::isxdigit(b[1])) {
            bytes[pos++] = static_cast<uint8_t>(strtoul(b, nullptr, 16));
        } else {
            break;
        }
    }

    return pos;
}

size_t read_hexstring(std::string &bytes, const std::string &str) {
    char b[3] = { '\0', '\0', '\0' };
    size_t pos = 0;

    bytes.clear();
    while ((2 * pos + 1) < str.length()) {
        b[0] = str[2 * pos];
        b[1] = str[2 * pos + 1];
        if (std::isxdigit(b[0]) && std::isxdigit(b[1])) {
            bytes.push_back(static_cast<char>(strtol(b, nullptr, 16)));
            pos++;
        } else {
            break;
        }
    }

    return pos;
}

inline char nibble_to_hex(uint8_t nibble) {
    static char char_table[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    if (nibble < 16) {
        return char_table[nibble];
    } else {
        return '?';
    }
}

void write_hexstring(std::string &str, const uint8_t *bytes, size_t len) {
    str.resize(2 * len);
    for (size_t pos = 0; pos < len; pos++) {
        str[2 * pos] = nibble_to_hex((bytes[pos] & 0xf0) >> 4);
        str[2 * pos + 1] = nibble_to_hex(bytes[pos] & 0x0f);
    }
}

void write_hexstring(std::string &str, const std::string &bytes) {
    uint8_t b;
    str.resize(2 * bytes.length());
    for (size_t pos = 0; pos < bytes.length(); pos++) {
        b = bytes[pos];
        str[2 * pos] = nibble_to_hex((b & 0xf0) >> 4);
        str[2 * pos + 1] = nibble_to_hex(b & 0x0f);
    }
}

/*#include <iostream>
#include <array>

// To test the functions above
int main(int argc, const char *argv[]) {
    if (argc == 2) {
        // std::chrono::system_clock::time_point epoch_sync;
        // std::cout << "system_clock ratio: " << std::chrono::system_clock::period::num
        //     << '/' << std::chrono::system_clock::period::den << std::endl;
        // std::cout << "sync_time status: " << sync_time(argv[1], epoch_sync) << std::endl;
        // std::chrono::seconds s;
        // s = std::chrono::duration_cast<std::chrono::seconds>(epoch_sync.time_since_epoch());
        // std::cout << epoch_sync.time_since_epoch().count() << "us or " << s.count() << 's' << std::endl;

        // uint8_t b[64];
        // std::string str{argv[1]};
        // size_t n = read_hexstring(b, 64, str);

        // std::cout << "Read " << n << " hex digits" << std::endl;

        // str.clear();
        // write_hexstring(str, b, n);

        // std::cout << "Wrote " << str << std::endl;

        std::vector<Peer> peers;
        Key key;
        std::string str;

        int r = load_config(argv[1], 1, peers, key);
        std::cout << "status " << r << std::endl;

        for (Peer &p : peers) {
            write_hexstring(str, p.key.data(), p.key.size());
            std::cout << "Host " << p.addr << " public key " << str << std::endl;
        }

        write_hexstring(str, key.data(), key.size());
        std::cout << "Read private key " << str << std::endl;
    }

    return 0;
}*/