#ifndef FLOWIE_CONTROL_CREDENTIAL_INTERNAL_H
#define FLOWIE_CONTROL_CREDENTIAL_INTERNAL_H

#include "flowie_control_store_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_CREDENTIAL_KDF_ARGON2ID 2
#define FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE 16
#define FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE 32

typedef struct flowie_control_credential_kdf_params_s {
  uint32_t algorithm;
  uint32_t memory_blocks;
  uint32_t passes;
  uint32_t lanes;
} flowie_control_credential_kdf_params_t;

void flowie_control_credential_default_params(flowie_control_credential_kdf_params_t *out);
int flowie_control_credential_params_valid(const flowie_control_credential_kdf_params_t *params);

int flowie_control_credential_generate(char token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY],
                                       uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                       uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
                                       const flowie_control_credential_kdf_params_t *params);
int flowie_control_credential_hash(const void *secret, size_t secret_size,
                                   uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                   uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
                                   const flowie_control_credential_kdf_params_t *params);

int flowie_control_credential_verify(
    const void *secret, size_t secret_size, const uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
    const uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
    const flowie_control_credential_kdf_params_t *params);

void flowie_control_credential_wipe(void *secret, size_t size);

#ifdef __cplusplus
}
#endif

#endif
