#include <iomanip>
#include <fstream>
#include <sstream>
#include <chrono>

int sync_time(const char *utc_start, std::chrono::system_clock::time_point &tpoint) {
    static_assert(std::chrono::system_clock::period::num == 1
        && std::chrono::system_clock::period::den >= 1000,
        "std::chrono::system_clock precision must be at least a millisecond");

    std::tm t_sync;
    char c;
    std::istringstream reader(utc_start);

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

    // system_clock::time_point tp = system_clock::now();

    std::time_t t_now;
    ::time(&t_now);
    const std::tm *tm = ::gmtime(&t_now);
    int now_sec = tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;

    // Start time must be in the future
    if (sync_sec <= now_sec) {
        return 2;
    }

    tpoint = std::chrono::system_clock::from_time_t(t_now) + std::chrono::seconds(sync_sec - now_sec);

    return 0;
}

/*#include <iostream>
#include <array>

// To test the functions above
int main(int argc, const char *argv[]) {
    if (argc == 2) {
        std::chrono::system_clock::time_point epoch_sync;
        std::cout << "system_clock ratio: " << std::chrono::system_clock::period::num
            << '/' << std::chrono::system_clock::period::den << std::endl;
        std::cout << "sync_time status: " << sync_time(argv[1], epoch_sync) << std::endl;
        std::chrono::seconds s;
        s = std::chrono::duration_cast<std::chrono::seconds>(epoch_sync.time_since_epoch());
        std::cout << epoch_sync.time_since_epoch().count() << "us or " << s.count() << 's' << std::endl;
    }

    return 0;
}*/