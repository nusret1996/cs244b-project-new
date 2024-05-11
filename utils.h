#include <chrono>

/*
 * Parses utc_start, which is expected to be a string of HH:MM:SS representing
 * a GMT time in the current day, and converts it to a std::chrono::system_clock::time_point
 * in local time. This is used to synchronize the start of the Streamlet epochs.
 * Returns 0 on success, 1 if utc_start is an invalid string, or 2 if utc_start represents
 * a time that has already passed.
 */
int sync_time(const char *utc_start, std::chrono::system_clock::time_point &tpoint);

/*
 * Reads at most len hex bytes from str. Each hex byte is two adjacent characters
 * representing a valid two hexademinal digit number. The hex values must be contiguous
 * (no whitespace) and reading will end at the first invalid or unmatched character.
 */
size_t read_hexstring(uint8_t *bytes, size_t len, const std::string &str);

/*
 * Writes len lowercase hex bytes corresponding to the elements of bytes
 * contiguously to str. str is resized to have length len and overwritten.
 */
void write_hexstring(std::string &str, const uint8_t *bytes, size_t len);