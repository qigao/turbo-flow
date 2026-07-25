/**
 * @file gf256.h
 * @brief GF(256) Reed-Solomon coding over the 0x11d primitive polynomial.
 *
 * Adapted to C from xtaci/libkcp commit
 * 824a449f6c966f247a8c7c2109e069c2383f360c.
 * Upstream files: fec.h, fec.cpp, reedsolomon.*, matrix.*, galois.*.
 * Copyright (c) 2016 Daniel Fu, MIT License.
 */
#ifndef MINIBLAS_GF256_H
#define MINIBLAS_GF256_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
  MINIBLAS_GF256_OK = 0,
  MINIBLAS_GF256_EINVAL = -1,
  MINIBLAS_GF256_ENOMEM = -2
};

typedef struct miniblas_gf256_rs_s {
  uint16_t data_shards;
  uint16_t parity_shards;
  uint16_t total_shards;
  uint8_t *matrix;
  uint8_t log_table[256];
  uint8_t exp_table[512];
} miniblas_gf256_rs_t;

/**
 * Initialize a systematic Reed-Solomon codec.
 * Time O((data_shards + parity_shards) * data_shards^2 + data_shards^3),
 * space O((data_shards + parity_shards) * data_shards).
 */
int miniblas_gf256_rs_init(miniblas_gf256_rs_t *codec,
                           uint16_t data_shards, uint16_t parity_shards);
void miniblas_gf256_rs_destroy(miniblas_gf256_rs_t *codec);

/**
 * XOR coefficient * input into output over GF(256).
 * Input and output may be unaligned but must not overlap.
 * Time O(size), space O(1).
 */
int miniblas_gf256_muladd(const miniblas_gf256_rs_t *codec,
                          uint8_t coefficient, const uint8_t *input,
                          uint8_t *output, size_t size);

/**
 * Fill parity shards from equally sized data shards.
 * Time O(data_shards * parity_shards * shard_size), space O(1).
 */
int miniblas_gf256_rs_encode(const miniblas_gf256_rs_t *codec,
                             uint8_t **shards, size_t shard_size);

/**
 * Reconstruct missing data shards from any data_shards present shards.
 * Missing output buffers may be NULL and are allocated by this function.
 * Time O(data_shards^3 + data_shards^2 * shard_size),
 * space O(data_shards^2 + data_shards * shard_size).
 */
int miniblas_gf256_rs_reconstruct_data(const miniblas_gf256_rs_t *codec,
                                       uint8_t **shards,
                                       const uint8_t *present,
                                       size_t shard_size);

#ifdef __cplusplus
}
#endif

#endif /* MINIBLAS_GF256_H */
