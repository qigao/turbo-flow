#include "flowie_control_credential_internal.h"

#include "base64_utils.h"
#include "platform.h"
#include "monocypher.h"
#include "turbo_error.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_ARGON2ID_ALGORITHM FLOWIE_CONTROL_CREDENTIAL_KDF_ARGON2ID
#define FLOWIE_CONTROL_ARGON2_MIN_MEMORY_BLOCKS 19456u
#define FLOWIE_CONTROL_ARGON2_MAX_MEMORY_BLOCKS 65536u
#define FLOWIE_CONTROL_ARGON2_DEFAULT_PASSES 2u
#define FLOWIE_CONTROL_ARGON2_MAX_PASSES 10u
#define FLOWIE_CONTROL_ARGON2_DEFAULT_LANES 1u
#define FLOWIE_CONTROL_ARGON2_MAX_LANES 4u
#define FLOWIE_CONTROL_ARGON2_BLOCK_SIZE 1024u
#define FLOWIE_CONTROL_CREDENTIAL_ENTROPY_BASE64_CAPACITY                                         \
  (4u * ((FLOWIE_CONTROL_CREDENTIAL_ENTROPY_SIZE + 2u) / 3u) + 1u)

void flowie_control_credential_default_params(flowie_control_credential_kdf_params_t *out) {
  if (!out) return;
  out->algorithm = FLOWIE_CONTROL_ARGON2ID_ALGORITHM;
  out->memory_blocks = FLOWIE_CONTROL_ARGON2_MIN_MEMORY_BLOCKS;
  out->passes = FLOWIE_CONTROL_ARGON2_DEFAULT_PASSES;
  out->lanes = FLOWIE_CONTROL_ARGON2_DEFAULT_LANES;
}

int flowie_control_credential_params_valid(const flowie_control_credential_kdf_params_t *params) {
  uint32_t lane_quads;
  if (!params || params->algorithm != FLOWIE_CONTROL_ARGON2ID_ALGORITHM ||
      params->memory_blocks < FLOWIE_CONTROL_ARGON2_MIN_MEMORY_BLOCKS ||
      params->memory_blocks > FLOWIE_CONTROL_ARGON2_MAX_MEMORY_BLOCKS || params->passes == 0u ||
      params->passes > FLOWIE_CONTROL_ARGON2_MAX_PASSES || params->lanes == 0u ||
      params->lanes > FLOWIE_CONTROL_ARGON2_MAX_LANES)
    return 0;
  lane_quads = params->lanes * 4u;
  return params->memory_blocks >= params->lanes * 8u && params->memory_blocks % lane_quads == 0u;
}

static int flowie_control_credential_derive(const void *secret, size_t secret_size,
                                            const uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                            const flowie_control_credential_kdf_params_t *params,
                                            uint8_t out[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE]) {
  crypto_argon2_config config;
  crypto_argon2_inputs inputs;
  void *work_area = NULL;
  size_t work_size;
  int rc = TURBO_OK;
  if ((!secret && secret_size != 0u) || !salt || !out ||
      !flowie_control_credential_params_valid(params) || secret_size > UINT32_MAX)
    return TURBO_EINVAL;
  if ((size_t)params->memory_blocks > SIZE_MAX / FLOWIE_CONTROL_ARGON2_BLOCK_SIZE)
    return TURBO_ERANGE;
  work_size = (size_t)params->memory_blocks * FLOWIE_CONTROL_ARGON2_BLOCK_SIZE;
  work_area = malloc(work_size);
  if (!work_area) return TURBO_ENOMEM;
  config.algorithm = params->algorithm;
  config.nb_blocks = params->memory_blocks;
  config.nb_passes = params->passes;
  config.nb_lanes = params->lanes;
  inputs.pass = (const uint8_t *)secret;
  inputs.salt = salt;
  inputs.pass_size = (uint32_t)secret_size;
  inputs.salt_size = FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE;
  crypto_argon2(out, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE, work_area, config, inputs,
                crypto_argon2_no_extras);

  crypto_wipe(work_area, work_size);
  free(work_area);
  return rc;
}

int flowie_control_credential_generate(char token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY],
                                       uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                       uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
                                       const flowie_control_credential_kdf_params_t *params) {
  uint8_t entropy[FLOWIE_CONTROL_CREDENTIAL_ENTROPY_SIZE] = {0};
  char encoded[FLOWIE_CONTROL_CREDENTIAL_ENTROPY_BASE64_CAPACITY] = {0};
  size_t index;
  int rc;
  if (!token || !salt || !verifier || !flowie_control_credential_params_valid(params))
    return TURBO_EINVAL;
  memset(token, 0, FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY);
  memset(salt, 0, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
  memset(verifier, 0, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE);
  rc = turbo_secure_random(entropy, sizeof(entropy));
  if (rc == TURBO_OK &&
      (tn_base64_encode_buf(entropy, sizeof(entropy), encoded, sizeof(encoded)) != 0 ||
       strlen(encoded) != FLOWIE_CONTROL_CREDENTIAL_TOKEN_PAYLOAD_SIZE + 1u ||
       encoded[FLOWIE_CONTROL_CREDENTIAL_TOKEN_PAYLOAD_SIZE] != '='))
    rc = TURBO_EIO;
  if (rc == TURBO_OK) {
    memcpy(token, FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX,
           FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX_SIZE);
    for (index = 0u; index < FLOWIE_CONTROL_CREDENTIAL_TOKEN_PAYLOAD_SIZE; ++index) {
      char value = encoded[index];
      token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_PREFIX_SIZE + index] =
          value == '+' ? '-' : (value == '/' ? '_' : value);
    }
    token[FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE] = '\0';
  }
  if (rc == TURBO_OK) rc = turbo_secure_random(salt, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
  if (rc == TURBO_OK)
    rc = flowie_control_credential_derive(token, FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE, salt, params,
                                          verifier);
  if (rc != TURBO_OK) {
    crypto_wipe(token, FLOWIE_CONTROL_CREDENTIAL_TOKEN_CAPACITY);
    crypto_wipe(salt, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
    crypto_wipe(verifier, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE);
  }
  crypto_wipe(entropy, sizeof(entropy));
  crypto_wipe(encoded, sizeof(encoded));
  return rc;
}

int flowie_control_credential_hash(const void *secret, size_t secret_size,
                                   uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
                                   uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
                                   const flowie_control_credential_kdf_params_t *params) {
  int rc;
  if (!secret || secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !salt ||
      !verifier || !flowie_control_credential_params_valid(params))
    return TURBO_EINVAL;
  memset(salt, 0, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
  memset(verifier, 0, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE);
  rc = turbo_secure_random(salt, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
  if (rc == TURBO_OK)
    rc = flowie_control_credential_derive(secret, secret_size, salt, params, verifier);
  if (rc != TURBO_OK) {
    crypto_wipe(salt, FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE);
    crypto_wipe(verifier, FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE);
  }
  return rc;
}

int flowie_control_credential_verify(
    const void *secret, size_t secret_size, const uint8_t salt[FLOWIE_CONTROL_CREDENTIAL_SALT_SIZE],
    const uint8_t verifier[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE],
    const flowie_control_credential_kdf_params_t *params) {
  uint8_t actual[FLOWIE_CONTROL_CREDENTIAL_VERIFIER_SIZE] = {0};
  int rc;
  if ((!secret && secret_size != 0u) || !salt || !verifier) return TURBO_EINVAL;
  rc = flowie_control_credential_derive(secret, secret_size, salt, params, actual);
  if (rc == TURBO_OK && crypto_verify32(actual, verifier) != 0) rc = TURBO_EPERM;
  crypto_wipe(actual, sizeof(actual));
  return rc;
}

void flowie_control_credential_wipe(void *secret, size_t size) {
  if (!secret || size == 0u) return;
  crypto_wipe(secret, size);
}
