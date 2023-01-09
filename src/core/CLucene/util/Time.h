#include <ctime>
#include <cstdint>

CL_NS_DEF(util)

#define NANOS_PER_SEC 1000000000ll
#define NANOS_PER_MILLIS 1000000ll
#define NANOS_PER_MICRO 1000ll
#define MICROS_PER_SEC 1000000ll
#define MICROS_PER_MILLI 1000ll
#define MILLIS_PER_SEC 1000ll
inline int64_t GetCurrentTimeMicros() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * MICROS_PER_SEC + ts.tv_nsec / NANOS_PER_MICRO;
}
inline int64_t UnixMillis() {
    return GetCurrentTimeMicros() / MICROS_PER_MILLI;
}
CL_NS_END