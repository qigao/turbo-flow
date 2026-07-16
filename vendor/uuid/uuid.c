// (‑●‑●)> dual licensed under the WTFPL v2 and MIT licenses
//   without any warranty.
//   by Gregory Pakosz (@gpakosz)
// https://github.com/gpakosz/uuid4

#if defined(__linux__)
  #if !defined(_GNU_SOURCE)
    #define _GNU_SOURCE
  #endif
#endif

// in case you want to #include "uuid4.c" in a larger compilation unit
#if !defined(UUID_H)
  #include "uuid.h"
#endif

#if !defined(UUID4_ASSERT)
  #include <assert.h>
  #define UUID4_ASSERT(expression) assert(expression)
#endif

#ifdef __cplusplus
extern "C" {
#endif

// —————————————————————————————————————————————————————————————————————————————

// http://xoshiro.di.unimi.it/splitmix64.c
// Written in 2015 by Sebastiano Vigna (vigna@acm.org)
/*
   This is a fixed-increment version of Java 8's SplittableRandom generator
   See http://dx.doi.org/10.1145/2714064.2660195 and
   http://docs.oracle.com/javase/8/docs/api/java/util/SplittableRandom.html

   It is a very fast generator passing BigCrush.
*/
static inline uint64_t UUID_PREFIX(splitmix64)(uint64_t *state) {
  uint64_t z = (*state += 0x9E3779B97F4A7C15u);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9u;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBu;
  return z ^ (z >> 31);
}

// http://www.pcg-random.org/posts/developing-a-seed_seq-alternative.html
// Written in 2015 by Melissa O'Neil (oneill@pcg-random.org)
static inline uint32_t UUID_PREFIX(hash)(uint32_t value) {
  static uint32_t multiplier = 0x43b0d7e5u;

  value ^= multiplier;
  multiplier *= 0x931e8875u;
  value *= multiplier;
  value ^= value >> 16;

  return value;
}

static inline uint32_t UUID_PREFIX(mix)(uint32_t x, uint32_t y) {
  uint32_t result = 0xca01f9ddu * x - 0x4973f715u * y;
  result ^= result >> 16;
  return result;
}

// —————————————————————————————————————————————————————————————————————————————

#if defined(_WIN32)

  #define WIN32_LEAN_AND_MEAN
  #include <windows.h>

UUID4_FUNCSPEC
void uuid_seed(uint64_t *state) {
  static uint64_t state0 = 0;

  LARGE_INTEGER time;
  BOOL ok = QueryPerformanceCounter(&time);
  UUID4_ASSERT(ok);

  *state = state0++ + ((uintptr_t)&time ^ (uint64_t)time.QuadPart);

  uint32_t pid = (uint32_t)GetCurrentProcessId();
  uint32_t tid = (uint32_t)GetCurrentThreadId();

  *state = *state * 6364136223846793005u +
           ((uint64_t)(UUID_PREFIX(mix)(UUID_PREFIX(hash)(pid), UUID_PREFIX(hash)(tid))) << 32);
  *state = *state * 6364136223846793005u + (uintptr_t)GetCurrentProcessId;
  *state = *state * 6364136223846793005u + (uintptr_t)uuid4_gen;
}

#elif defined(__linux__)

  #if !defined(UUID4_CLOCK_ID)
    #define UUID4_CLOCK_ID CLOCK_MONOTONIC_RAW
  #endif

  #include <sys/syscall.h>
  #include <time.h>
  #include <unistd.h>

UUID4_FUNCSPEC
void uuid_seed(uint64_t *state) {
  static uint64_t state0 = 0;

  struct timespec time;
  bool ok = clock_gettime(UUID4_CLOCK_ID, &time) == 0;
  UUID4_ASSERT(ok);

  *state = state0++ + ((uintptr_t)&time ^ (uint64_t)(time.tv_sec * 1000000000 + time.tv_nsec));

  uint32_t pid = (uint32_t)getpid();
  uint32_t tid = (uint32_t)syscall(SYS_gettid);
  *state = *state * 6364136223846793005u +
           ((uint64_t)(UUID_PREFIX(mix)(UUID_PREFIX(hash)(pid), UUID_PREFIX(hash)(tid))) << 32);
  *state = *state * 6364136223846793005u + (uintptr_t)getpid;
  *state = *state * 6364136223846793005u + (uintptr_t)uuid4_gen;
}

#elif defined(__APPLE__)

  #include <mach/mach_time.h>
  #include <pthread.h>
  #include <sys/time.h>
  #include <time.h>
  #include <unistd.h>

UUID4_FUNCSPEC
void uuid_seed(uint64_t *state) {
  static uint64_t state0 = 0;

  uint64_t time = mach_absolute_time();

  *state = state0++ + time;

  uint32_t pid = (uint32_t)getpid();
  uint64_t tid;
  pthread_threadid_np(NULL, &tid);
  *state = *state * 6364136223846793005u +
           ((uint64_t)(UUID_PREFIX(mix)(UUID_PREFIX(hash)(pid), UUID_PREFIX(hash)((uint32_t)tid)))
            << 32);
  *state = *state * 6364136223846793005u + (uintptr_t)getpid;
  *state = *state * 6364136223846793005u + (uintptr_t)uuid4_gen;
}

#endif

static uint64_t UUID_PREFIX(unix_time_ms)() {
#if defined(_WIN32)
  FILETIME ft;
  GetSystemTimePreciseAsFileTime(&ft);
  ULARGE_INTEGER li;
  li.LowPart = ft.dwLowDateTime;
  li.HighPart = ft.dwHighDateTime;
  // Windows file time is 100ns intervals since Jan 1, 1601.
  // Unix epoch is Jan 1, 1970. Difference is 11644473600 seconds.
  return (li.QuadPart - 116444736000000000ULL) / 10000;
#elif defined(__linux__) || defined(__APPLE__)
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
#else
  #error unsupported platform
#endif
}

#include <inttypes.h>
#include <stdbool.h>

static void UUID_PREFIX(randomize)(uuid_state_t *state, uuid_t *out) {
  out->qwords[0] = UUID_PREFIX(splitmix64)(state);
  out->qwords[1] = UUID_PREFIX(splitmix64)(state);
}

UUID4_FUNCSPEC
void uuid4_gen(uuid_state_t *state, uuid_t *out) {
  UUID_PREFIX(randomize)(state, out);

  out->bytes[6] = (out->bytes[6] & 0xf) | 0x40;
  out->bytes[8] = (out->bytes[8] & 0x3f) | 0x80;
}

UUID4_FUNCSPEC
void uuid7_gen(uuid_state_t *state, uuid_t *out) {
  uint64_t timestamp = UUID_PREFIX(unix_time_ms)();

  UUID_PREFIX(randomize)(state, out);

  // v7: 48-bit timestamp (unix epoch ms), 4-bit ver (0x7), 72-bit rand
  out->bytes[0] = (uint8_t)(timestamp >> 40);
  out->bytes[1] = (uint8_t)(timestamp >> 32);
  out->bytes[2] = (uint8_t)(timestamp >> 24);
  out->bytes[3] = (uint8_t)(timestamp >> 16);
  out->bytes[4] = (uint8_t)(timestamp >> 8);
  out->bytes[5] = (uint8_t)timestamp;

  out->bytes[6] = (out->bytes[6] & 0x0f) | 0x70; // Set version bits (v7)
  out->bytes[8] = (out->bytes[8] & 0x3f) | 0x80; // Set variant bits (RFC4122)
}

UUID4_FUNCSPEC
bool uuid_to_s(const uuid_t uuid, char *out, int capacity) {
  static const char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                             '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  static const int groups[] = {8, 4, 4, 4, 12};
  int b = 0;

  if (capacity < UUID4_STR_BUFFER_SIZE)
    return false;

  for (int i = 0; i < (int)(sizeof(groups) / sizeof(groups[0])); ++i) {
    for (int j = 0; j < groups[i]; j += 2) {
      uint8_t byte = uuid.bytes[b++];

      *out++ = hex[byte >> 4];
      *out++ = hex[byte & 0xf];
    }
    *out++ = '-';
  }

  *--out = 0;

  return true;
}

UUID4_FUNCSPEC
bool uuid_from_s(const char *in, uuid_t *out) {
  for (int i = 0; i < 32; i++) {
    char c = *in++;
    uint8_t x = 0xff;

    if (c >= '0' && c <= '9')
      x = c - '0';
    else if (c >= 'a' && c <= 'f')
      x = c - 'a' + 10;
    else if (c >= 'A' && c <= 'F')
      x = c - 'A' + 10;

    if (x == 0xff)
      return false;

    if ((i & 1) == 0)
      out->bytes[i >> 1] = x << 4;
    else
      out->bytes[i >> 1] |= x;

    if ((i == 7 || i == 11 || i == 15 || i == 19) && (*in++ != '-'))
      return false;
  }

  return (*in == '\0');
}

#if defined(UUID4_PRACTRAND_TEST)

// $ gcc -O2 -Wall -Werror -DUUID4_PRACTPRAND_TEST -o uuid4_practrand_test uuid.c

  #include <stdio.h>
  #include <stdlib.h>

  #ifdef _WIN32
    #include <fcntl.h>
    #include <io.h>
  #else
    #include <unistd.h>
  #endif

int main() {
  if (isatty(fileno(stdout))) {
    fprintf(stderr, "usage: uuid4_practrand_test | RNG_test stdin64\n");
    exit(EXIT_FAILURE);
  }

  #ifdef _WIN32
  _setmode(fileno(stdout), _O_BINARY);
  #endif

  uuid_state_t state;
  uuid_seed(&state);

  while (true) {
    uuid_t uuid[1024];
    for (size_t i = 0; i < sizeof(uuid) / sizeof(uuid[0]); ++i)
      UUID_PREFIX(randomize)(&state, &uuid[i]);

    fwrite(uuid, sizeof(uuid), 1, stdout);
  }

  return 0;
}

#elif defined(UUID4_TESTU01_TEST)

  #include <TestU01.h>
  #include <stdlib.h>

static inline uint32_t rev32(uint32_t v) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // swap odd and even bits
  v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
  // swap consecutive pairs
  v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
  // swap nibbles ...
  v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
  // swap bytes
  v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
  // swap 2-byte-long pairs
  v = (v >> 16) | (v << 16);
  return v;
}

static uuid_state_t state;

static unsigned int gen_uuid_0() {
  uuid_t uuid;
  UUID_PREFIX(randomize)(&state, &uuid);

  return uuid.dwords[0];
}

static unsigned int gen_uuid_0_rev() { return rev32(gen_uuid_0()); }

static unsigned int gen_uuid_1() {
  uuid_t uuid;
  UUID_PREFIX(randomize)(&state, &uuid);

  return uuid.dwords[1];
}

static unsigned int gen_uuid_1_rev() { return rev32(gen_uuid_1()); }

static unsigned int gen_uuid_2() {
  uuid_t uuid;
  UUID_PREFIX(randomize)(&state, &uuid);

  return uuid.dwords[2];
}

static unsigned int gen_uuid_2_rev() { return rev32(gen_uuid_2()); }

static unsigned int gen_uuid_3() {
  uuid_t uuid;
  UUID_PREFIX(randomize)(&state, &uuid);

  return uuid.dwords[3];
}

static unsigned int gen_uuid_3_rev() { return rev32(gen_uuid_3()); }

int main(int argc, char *argv[]) {
  swrite_Basic = FALSE;

  uuid_seed(&state);

  struct {
    const char *name;
    unsigned int (*gen)();
  } gens[] = {{"uuid4.dwords[0]", gen_uuid_0}, {"uuid4.dwords[0] (reversed)", gen_uuid_0_rev},
              {"uuid4.dwords[1]", gen_uuid_1}, {"uuid4.dwords[1] (reversed)", gen_uuid_1_rev},
              {"uuid4.dwords[2]", gen_uuid_2}, {"uuid4.dwords[2] (reversed)", gen_uuid_2_rev},
              {"uuid4.dwords[3]", gen_uuid_3}, {"uuid4.dwords[3] (reversed)", gen_uuid_3_rev}};

  for (size_t i = 0; i < sizeof(gens) / sizeof(gens[0]); ++i) {
    unif01_Gen *gen = unif01_CreateExternGenBits((char *)gens[i].name, gens[i].gen);
    bbattery_SmallCrush(gen);
    unif01_DeleteExternGenBits(gen);
  }

  for (size_t i = 0; i < sizeof(gens) / sizeof(gens[0]); ++i) {
    unif01_Gen *gen = unif01_CreateExternGenBits((char *)gens[i].name, gens[i].gen);
    bbattery_Crush(gen);
    unif01_DeleteExternGenBits(gen);
  }

  for (size_t i = 0; i < sizeof(gens) / sizeof(gens[0]); ++i) {
    unif01_Gen *gen = unif01_CreateExternGenBits((char *)gens[i].name, gens[i].gen);
    bbattery_BigCrush(gen);
    unif01_DeleteExternGenBits(gen);
  }

  return 0;
}

#endif

#ifdef __cplusplus
}
#endif
