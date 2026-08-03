#ifndef FLOWIE_CLUSTER_PEER_WIRE_INTERNAL_H
#define FLOWIE_CLUSTER_PEER_WIRE_INTERNAL_H

#include <stdint.h>

static inline void flowie_cluster_peer_wire_write_u16(uint8_t *out, uint16_t value) {
  out[0] = (uint8_t)(value >> 8u);
  out[1] = (uint8_t)value;
}

static inline void flowie_cluster_peer_wire_write_u32(uint8_t *out, uint32_t value) {
  out[0] = (uint8_t)(value >> 24u);
  out[1] = (uint8_t)(value >> 16u);
  out[2] = (uint8_t)(value >> 8u);
  out[3] = (uint8_t)value;
}

static inline void flowie_cluster_peer_wire_write_u64(uint8_t *out, uint64_t value) {
  unsigned int index;
  for (index = 0u; index < 8u; ++index)
    out[index] = (uint8_t)(value >> (56u - index * 8u));
}

static inline uint16_t flowie_cluster_peer_wire_read_u16(const uint8_t *data) {
  return (uint16_t)(((uint16_t)data[0] << 8u) | data[1]);
}

static inline uint32_t flowie_cluster_peer_wire_read_u32(const uint8_t *data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) | ((uint32_t)data[2] << 8u) |
         data[3];
}

static inline uint64_t flowie_cluster_peer_wire_read_u64(const uint8_t *data) {
  uint64_t value = 0u;
  unsigned int index;
  for (index = 0u; index < 8u; ++index)
    value = (value << 8u) | data[index];
  return value;
}

#endif /* FLOWIE_CLUSTER_PEER_WIRE_INTERNAL_H */
