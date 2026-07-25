#include "gf256.h"
#include "tinytest.h"

#include <stdint.h>
#include <string.h>

static uint8_t gf256_reference_multiply(uint8_t a, uint8_t b) {
    uint8_t result = 0;
    while (b != 0) {
        if (b & 1U) result ^= a;
        b >>= 1;
        a = (uint8_t)((a << 1) ^ ((a & 0x80U) ? 0x1dU : 0U));
    }
    return result;
}

spec("miniblas GF(256)") {
    it("should match scalar GF(256) multiplication for every byte pair") {
        miniblas_gf256_rs_t codec;
        uint8_t input[257];
        uint8_t output[259];
        unsigned coefficient;
        unsigned value;

        check_int_eq(miniblas_gf256_rs_init(&codec, 2, 1), MINIBLAS_GF256_OK);
        for (value = 0; value < 256U; ++value) input[value + 1U] = (uint8_t)value;

        for (coefficient = 0; coefficient < 256U; ++coefficient) {
            memset(output, 0xa5, sizeof(output));
            check_int_eq(miniblas_gf256_muladd(
                             &codec, (uint8_t)coefficient, input + 1,
                             output + 1, 256), MINIBLAS_GF256_OK);
            for (value = 0; value < 256U; ++value) {
                check_int_eq(output[value + 1U],
                             (uint8_t)(0xa5U ^ gf256_reference_multiply(
                                 (uint8_t)coefficient, (uint8_t)value)));
            }
            check_int_eq(output[0], 0xa5);
            check_int_eq(output[257], 0xa5);
        }
        miniblas_gf256_rs_destroy(&codec);
    }

    it("should process unaligned input and scalar tails") {
        miniblas_gf256_rs_t codec;
        uint8_t input[39];
        uint8_t output[41];
        size_t i;

        check_int_eq(miniblas_gf256_rs_init(&codec, 3, 2), MINIBLAS_GF256_OK);
        for (i = 0; i < sizeof(input); ++i) input[i] = (uint8_t)(i * 7U + 3U);
        memset(output, 0x3c, sizeof(output));
        check_int_eq(miniblas_gf256_muladd(&codec, 173, input + 1,
                                           output + 2, 37),
                     MINIBLAS_GF256_OK);
        for (i = 0; i < 37; ++i) {
            check_int_eq(output[i + 2],
                         (uint8_t)(0x3cU ^ gf256_reference_multiply(
                             173, input[i + 1])));
        }
        check_int_eq(output[1], 0x3c);
        check_int_eq(output[39], 0x3c);
        miniblas_gf256_rs_destroy(&codec);
    }
}
