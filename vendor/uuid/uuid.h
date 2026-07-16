#ifndef UUID4_H
#define UUID4_H

#ifdef __cplusplus
  #include <cstdint>
extern "C" {
#else
  #include <stdbool.h>
  #include <stdint.h>
#endif

#ifndef UUID4_FUNCSPEC
  #define UUID4_FUNCSPEC
#endif
#ifndef UUID_PREFIX
  #define UUID_PREFIX(x) uuid_##x
#endif

#ifndef UUID4_STR_BUFFER_SIZE
  #define UUID4_STR_BUFFER_SIZE                                                                    \
    (int)sizeof("xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx") // y is either 8, 9, a or b
#endif

typedef uint64_t uuid_state_t;
#define UUID_STATE_T uuid_state_t

typedef union {
  uint8_t bytes[16];
  uint32_t dwords[4];
  uint64_t qwords[2];
} uuid_t;
#define UUID_T uuid_t

/**
 * Seeds the state of the PRNG used to generate UUIDs.
 *
 * @param seed a pointer to a variable holding the state.
 */
UUID4_FUNCSPEC
void uuid_seed(uuid_state_t *seed);

/**
 * Generates a version 4 UUID, see https://tools.ietf.org/html/rfc4122.
 *
 * @param state the state of the PRNG used to generate version 4 UUIDs.
 * @param out the recipient for the UUID.
 */
UUID4_FUNCSPEC
void uuid4_gen(uuid_state_t *state, uuid_t *out);

/**
 * Generates a version 7 UUID, see
 * https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis-03
 *
 * @param state the state of the PRNG used to generate version 7 UUIDs.
 * @param out the recipient for the UUID.
 */
UUID4_FUNCSPEC
void uuid7_gen(uuid_state_t *state, uuid_t *out);

/**
 * Converts a UUID to a a `NUL` terminated string.
 *
 * @param out destination buffer
 * @param capacity destination buffer capacity, must be greater or equal to
 *   `UUID4_STR_BUFFER_SIZE`.
 *
 * @return `true` on success, otherwise `false`.
 */
UUID4_FUNCSPEC
bool uuid_to_s(const uuid_t uuid, char *out, int capacity);

/**
 * Converts a string to a UUID.
 *
 * @param in input string (8-4-4-4-12 format)
 * @param out destination UUID
 *
 * @return `true` on success, otherwise `false`.
 */
UUID4_FUNCSPEC
bool uuid_from_s(const char *in, uuid_t *out);

#ifdef __cplusplus
}
#endif

#endif // #ifndef UUID4_H
