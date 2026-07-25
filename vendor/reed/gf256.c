#include "gf256.h"

#include <stdlib.h>
#include <string.h>

#include <simde/x86/sse2.h>

static uint8_t gf_mul(const miniblas_gf256_rs_t *codec, uint8_t a, uint8_t b) {
  if (a == 0 || b == 0) return 0;
  return codec->exp_table[(unsigned)codec->log_table[a] + codec->log_table[b]];
}

static uint8_t gf_div(const miniblas_gf256_rs_t *codec, uint8_t a, uint8_t b) {
  int exponent;
  if (a == 0) return 0;
  if (b == 0) return 0;
  exponent = (int)codec->log_table[a] - (int)codec->log_table[b];
  if (exponent < 0) exponent += 255;
  return codec->exp_table[exponent];
}

static uint8_t gf_pow(const miniblas_gf256_rs_t *codec, uint8_t a,
                      unsigned power) {
  if (power == 0) return 1;
  if (a == 0) return 0;
  return codec->exp_table[((unsigned)codec->log_table[a] * power) % 255U];
}

static int invert_matrix(const miniblas_gf256_rs_t *codec,
                         const uint8_t *input, uint8_t *output,
                         unsigned size) {
  const unsigned width = size * 2U;
  uint8_t *work = (uint8_t *)calloc((size_t)size, width);
  unsigned row;
  unsigned col;
  if (!work) return MINIBLAS_GF256_ENOMEM;

  for (row = 0; row < size; ++row) {
    memcpy(work + (size_t)row * width,
           input + (size_t)row * size, size);
    work[(size_t)row * width + size + row] = 1;
  }
  for (row = 0; row < size; ++row) {
    uint8_t *pivot = work + (size_t)row * width;
    if (pivot[row] == 0) {
      unsigned swap_row;
      for (swap_row = row + 1U; swap_row < size; ++swap_row) {
        if (work[(size_t)swap_row * width + row] != 0) break;
      }
      if (swap_row == size) {
        free(work);
        return MINIBLAS_GF256_EINVAL;
      }
      for (col = 0; col < width; ++col) {
        uint8_t tmp = pivot[col];
        pivot[col] = work[(size_t)swap_row * width + col];
        work[(size_t)swap_row * width + col] = tmp;
      }
    }
    if (pivot[row] != 1) {
      uint8_t scale = gf_div(codec, 1, pivot[row]);
      for (col = 0; col < width; ++col) {
        pivot[col] = gf_mul(codec, pivot[col], scale);
      }
    }
    {
      unsigned other;
      for (other = 0; other < size; ++other) {
        uint8_t *target;
        uint8_t scale;
        if (other == row) continue;
        target = work + (size_t)other * width;
        scale = target[row];
        if (scale == 0) continue;
        for (col = 0; col < width; ++col) {
          target[col] ^= gf_mul(codec, scale, pivot[col]);
        }
      }
    }
  }
  for (row = 0; row < size; ++row) {
    memcpy(output + (size_t)row * size,
           work + (size_t)row * width + size, size);
  }
  free(work);
  return MINIBLAS_GF256_OK;
}

void miniblas_gf256_rs_destroy(miniblas_gf256_rs_t *codec) {
  if (!codec) return;
  free(codec->matrix);
  memset(codec, 0, sizeof(*codec));
}

int miniblas_gf256_rs_init(miniblas_gf256_rs_t *codec,
                           uint16_t data_shards,
                           uint16_t parity_shards) {
  uint8_t *vandermonde;
  uint8_t *top_inverse;
  unsigned total;
  unsigned row;
  unsigned col;
  unsigned i;
  uint16_t value;
  int rc;

  if (!codec || data_shards == 0 || parity_shards == 0 ||
      (unsigned)data_shards + parity_shards > 255U) {
    return MINIBLAS_GF256_EINVAL;
  }
  memset(codec, 0, sizeof(*codec));
  codec->data_shards = data_shards;
  codec->parity_shards = parity_shards;
  codec->total_shards = (uint16_t)(data_shards + parity_shards);

  value = 1;
  for (i = 0; i < 255U; ++i) {
    codec->exp_table[i] = (uint8_t)value;
    codec->log_table[(uint8_t)value] = (uint8_t)i;
    value <<= 1;
    if (value & 0x100U) value ^= 0x11dU;
  }
  for (i = 255U; i < 512U; ++i) {
    codec->exp_table[i] = codec->exp_table[i - 255U];
  }

  total = codec->total_shards;
  vandermonde = (uint8_t *)calloc((size_t)total, data_shards);
  top_inverse = (uint8_t *)calloc((size_t)data_shards, data_shards);
  codec->matrix = (uint8_t *)calloc((size_t)total, data_shards);
  if (!vandermonde || !top_inverse || !codec->matrix) {
    free(vandermonde);
    free(top_inverse);
    miniblas_gf256_rs_destroy(codec);
    return MINIBLAS_GF256_ENOMEM;
  }
  for (row = 0; row < total; ++row) {
    for (col = 0; col < data_shards; ++col) {
      vandermonde[(size_t)row * data_shards + col] =
          gf_pow(codec, (uint8_t)row, col);
    }
  }
  rc = invert_matrix(codec, vandermonde, top_inverse, data_shards);
  if (rc == MINIBLAS_GF256_OK) {
    for (row = 0; row < total; ++row) {
      for (col = 0; col < data_shards; ++col) {
        uint8_t result = 0;
        for (i = 0; i < data_shards; ++i) {
          result ^= gf_mul(codec,
              vandermonde[(size_t)row * data_shards + i],
              top_inverse[(size_t)i * data_shards + col]);
        }
        codec->matrix[(size_t)row * data_shards + col] = result;
      }
    }
  }
  free(vandermonde);
  free(top_inverse);
  if (rc != MINIBLAS_GF256_OK) miniblas_gf256_rs_destroy(codec);
  return rc;
}

int miniblas_gf256_muladd(const miniblas_gf256_rs_t *codec,
                          uint8_t coefficient, const uint8_t *input,
                          uint8_t *output, size_t size) {
  size_t offset = 0;
  simde__m128i reduction = simde_mm_set1_epi8(0x1d);
  simde__m128i zero = simde_mm_setzero_si128();

  if (!codec || !input || !output) return MINIBLAS_GF256_EINVAL;
  if (coefficient == 0 || size == 0) return MINIBLAS_GF256_OK;
  if (coefficient == 1) {
    for (; offset + 16U <= size; offset += 16U) {
      simde__m128i in = simde_mm_loadu_si128(input + offset);
      simde__m128i out = simde_mm_loadu_si128(output + offset);
      simde_mm_storeu_si128(output + offset, simde_mm_xor_si128(out, in));
    }
    for (; offset < size; ++offset) output[offset] ^= input[offset];
    return MINIBLAS_GF256_OK;
  }

  for (; offset + 16U <= size; offset += 16U) {
    simde__m128i multiplicand = simde_mm_loadu_si128(input + offset);
    simde__m128i product = zero;
    uint8_t multiplier = coefficient;
    unsigned bit;
    for (bit = 0; bit < 8U; ++bit) {
      simde__m128i high_bit;
      if (multiplier & 1U) product = simde_mm_xor_si128(product, multiplicand);
      high_bit = simde_mm_cmpgt_epi8(zero, multiplicand);
      multiplicand = simde_mm_xor_si128(
          simde_mm_add_epi8(multiplicand, multiplicand),
          simde_mm_and_si128(high_bit, reduction));
      multiplier >>= 1;
    }
    simde__m128i out = simde_mm_loadu_si128(output + offset);
    simde_mm_storeu_si128(output + offset,
                          simde_mm_xor_si128(out, product));
  }
  for (; offset < size; ++offset) {
    output[offset] ^= gf_mul(codec, coefficient, input[offset]);
  }
  return MINIBLAS_GF256_OK;
}

static void code_row(const miniblas_gf256_rs_t *codec, const uint8_t *row,
                     uint8_t **inputs, uint8_t *output,
                     size_t shard_size) {
  unsigned input;
  memset(output, 0, shard_size);
  for (input = 0; input < codec->data_shards; ++input) {
    (void)miniblas_gf256_muladd(codec, row[input], inputs[input], output,
                               shard_size);
  }
}

int miniblas_gf256_rs_encode(const miniblas_gf256_rs_t *codec,
                             uint8_t **shards, size_t shard_size) {
  unsigned i;
  if (!codec || !codec->matrix || !shards || shard_size == 0) {
    return MINIBLAS_GF256_EINVAL;
  }
  for (i = 0; i < codec->total_shards; ++i) {
    if (!shards[i]) return MINIBLAS_GF256_EINVAL;
  }
  for (i = 0; i < codec->parity_shards; ++i) {
    unsigned row = codec->data_shards + i;
    code_row(codec, codec->matrix + (size_t)row * codec->data_shards,
             shards, shards[row], shard_size);
  }
  return MINIBLAS_GF256_OK;
}

int miniblas_gf256_rs_reconstruct_data(const miniblas_gf256_rs_t *codec,
                                       uint8_t **shards,
                                       const uint8_t *present,
                                       size_t shard_size) {
  uint8_t *submatrix;
  uint8_t *inverse;
  uint8_t **inputs;
  unsigned selected = 0;
  unsigned shard;
  int rc;

  if (!codec || !codec->matrix || !shards || !present || shard_size == 0) {
    return MINIBLAS_GF256_EINVAL;
  }
  submatrix = (uint8_t *)calloc((size_t)codec->data_shards,
                                codec->data_shards);
  inverse = (uint8_t *)calloc((size_t)codec->data_shards,
                              codec->data_shards);
  inputs = (uint8_t **)calloc(codec->data_shards, sizeof(*inputs));
  if (!submatrix || !inverse || !inputs) {
    free(submatrix);
    free(inverse);
    free(inputs);
    return MINIBLAS_GF256_ENOMEM;
  }
  for (shard = 0;
       shard < codec->total_shards && selected < codec->data_shards;
       ++shard) {
    if (!present[shard] || !shards[shard]) continue;
    memcpy(submatrix + (size_t)selected * codec->data_shards,
           codec->matrix + (size_t)shard * codec->data_shards,
           codec->data_shards);
    inputs[selected++] = shards[shard];
  }
  if (selected < codec->data_shards) {
    free(submatrix);
    free(inverse);
    free(inputs);
    return MINIBLAS_GF256_EINVAL;
  }
  rc = invert_matrix(codec, submatrix, inverse, codec->data_shards);
  if (rc == MINIBLAS_GF256_OK) {
    for (shard = 0; shard < codec->data_shards; ++shard) {
      if (!present[shard]) {
        if (!shards[shard]) {
          shards[shard] = (uint8_t *)calloc(1, shard_size);
          if (!shards[shard]) {
            rc = MINIBLAS_GF256_ENOMEM;
            break;
          }
        }
        code_row(codec, inverse + (size_t)shard * codec->data_shards,
                 inputs, shards[shard], shard_size);
      }
    }
  }
  free(submatrix);
  free(inverse);
  free(inputs);
  return rc;
}
