#include <chrono>

/*
 * Parses utc_start, which is expected to be a string of HH:MM:SS representing
 * a GMT time in the current day, and converts it to a std::chrono::system_clock::time_point
 * in local time. This is used to synchronize the start of the Streamlet epochs.
 * Returns 0 on success, 1 if utc_start is an invalid string, or 2 if utc_start represents
 * a time that has already passed.
 */
int sync_time(const char *utc_start, std::chrono::system_clock::time_point &tpoint);