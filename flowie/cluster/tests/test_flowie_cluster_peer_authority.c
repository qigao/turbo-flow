#include "flowie_cluster_peer_authority_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

typedef struct authority_fixture_s {
  flowie_cluster_peer_certificate_pin_t pins[2];
  flowie_cluster_topology_peer_t peers[2];
  uint8_t boots[2][FLOWIE_CLUSTER_BOOT_ID_SIZE];
  char fingerprints[2][FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE];
} authority_fixture_t;

static void authority_fingerprint(char output[FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE],
                                  char digit) {
  memcpy(output, "sha256:", 7u);
  memset(output + 7u, digit, 64u);
  output[71] = '\0';
}

static void authority_fixture_init(authority_fixture_t *fixture) {
  memset(fixture, 0, sizeof(*fixture));
  authority_fingerprint(fixture->fingerprints[0], '1');
  authority_fingerprint(fixture->fingerprints[1], '2');
  for (size_t index = 0u; index < 2u; ++index) {
    fixture->pins[index] =
        (flowie_cluster_peer_certificate_pin_t)FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT;
    fixture->pins[index].node_id = tstr_v_from_cstr(index == 0u ? "node-a" : "node-b");
    fixture->pins[index].certificate_sha256 = fixture->fingerprints[index];
    fixture->boots[index][0] = (uint8_t)(index + 1u);
    fixture->peers[index] = (flowie_cluster_topology_peer_t)FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
    fixture->peers[index].node_id = fixture->pins[index].node_id;
    memcpy(fixture->peers[index].boot_id, fixture->boots[index],
           sizeof(fixture->peers[index].boot_id));
    fixture->peers[index].advertised_endpoint =
        tstr_v_from_cstr(index == 0u ? "10.0.0.1:7100" : "10.0.0.2:7100");
  }
}

static flowie_cluster_peer_authority_t *authority_fixture_create(authority_fixture_t *fixture) {
  flowie_cluster_peer_authority_config_t config = FLOWIE_CLUSTER_PEER_AUTHORITY_CONFIG_INIT;
  flowie_cluster_peer_authority_t *authority = NULL;
  config.max_peers = 2u;
  config.pins = fixture->pins;
  config.pin_count = 2u;
  check_int_eq(flowie_cluster_peer_authority_create(&config, &authority), TURBO_OK);
  check_not_null(authority);
  return authority;
}

spec("Flowie cluster peer authority") {
  it("authorizes only the exact active node boot and verified certificate") {
    authority_fixture_t fixture;
    flowie_cluster_peer_authority_t *authority;
    authority_fixture_init(&fixture);
    authority = authority_fixture_create(&fixture);
    check_int_eq(flowie_cluster_peer_authority_replace(authority, fixture.peers, 2u, 7u),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_authority_authorize(
                     authority, fixture.peers[0].node_id, fixture.boots[0],
                     fixture.fingerprints[0]),
                 TURBO_OK);
    fixture.boots[0][1] = 9u;
    check_int_eq(flowie_cluster_peer_authority_authorize(
                     authority, fixture.peers[0].node_id, fixture.boots[0],
                     fixture.fingerprints[0]),
                 TURBO_EPERM);
    fixture.boots[0][1] = 0u;
    check_int_eq(flowie_cluster_peer_authority_authorize(
                     authority, fixture.peers[0].node_id, fixture.boots[0],
                     fixture.fingerprints[1]),
                 TURBO_EPERM);
    check_int_eq(flowie_cluster_peer_authority_authorize(
                     authority, tstr_v_from_cstr("node-c"), fixture.boots[0],
                     fixture.fingerprints[0]),
                 TURBO_EPERM);
    flowie_cluster_peer_authority_destroy(authority);
  }

  it("publishes an atomic caller-owned topology snapshot") {
    authority_fixture_t fixture;
    flowie_cluster_topology_peer_t snapshot[2];
    flowie_cluster_peer_authority_t *authority;
    size_t count = 0u;
    uint64_t revision = 0u;
    authority_fixture_init(&fixture);
    authority = authority_fixture_create(&fixture);
    check_int_eq(flowie_cluster_peer_authority_replace(authority, fixture.peers, 2u, 11u),
                 TURBO_OK);
    check_int_eq(flowie_cluster_peer_authority_snapshot(authority, snapshot, 1u, &count,
                                                        &revision),
                 TURBO_ENOSPC);
    check_int_eq(flowie_cluster_peer_authority_snapshot(authority, snapshot, 2u, &count,
                                                        &revision),
                 TURBO_OK);
    check_int_eq(count, 2u);
    check_int_eq(revision, 11u);
    check_int_eq(snapshot[0].node_id.len, strlen("node-a"));
    check_int_eq(memcmp(snapshot[0].node_id.data, "node-a", strlen("node-a")), 0);
    check_int_eq(memcmp(snapshot[1].boot_id, fixture.boots[1], sizeof(snapshot[1].boot_id)), 0);
    flowie_cluster_peer_authority_destroy(authority);
  }

  it("rejects unknown or unordered membership without replacing authority") {
    authority_fixture_t fixture;
    flowie_cluster_topology_peer_t reordered[2];
    flowie_cluster_topology_peer_t snapshot[2];
    flowie_cluster_peer_authority_t *authority;
    size_t count = 0u;
    uint64_t revision = 0u;
    authority_fixture_init(&fixture);
    authority = authority_fixture_create(&fixture);
    check_int_eq(flowie_cluster_peer_authority_replace(authority, fixture.peers, 1u, 3u),
                 TURBO_OK);
    reordered[0] = fixture.peers[1];
    reordered[1] = fixture.peers[0];
    check_int_eq(flowie_cluster_peer_authority_replace(authority, reordered, 2u, 4u),
                 TURBO_EINVAL);
    fixture.peers[1].node_id = tstr_v_from_cstr("node-c");
    check_int_eq(flowie_cluster_peer_authority_replace(authority, fixture.peers, 2u, 4u),
                 TURBO_EPERM);
    check_int_eq(flowie_cluster_peer_authority_snapshot(authority, snapshot, 2u, &count,
                                                        &revision),
                 TURBO_OK);
    check_int_eq(count, 1u);
    check_int_eq(revision, 3u);
    flowie_cluster_peer_authority_destroy(authority);
  }

  it("requires canonical unique certificate pins and bounded capacity") {
    authority_fixture_t fixture;
    flowie_cluster_peer_authority_config_t config = FLOWIE_CLUSTER_PEER_AUTHORITY_CONFIG_INIT;
    flowie_cluster_peer_authority_t *authority = NULL;
    authority_fixture_init(&fixture);
    config.max_peers = 2u;
    config.pins = fixture.pins;
    config.pin_count = 2u;
    fixture.fingerprints[0][7] = 'A';
    check_int_eq(flowie_cluster_peer_authority_create(&config, &authority), TURBO_EINVAL);
    check_null(authority);
    fixture.fingerprints[0][7] = '1';
    fixture.pins[1].node_id = fixture.pins[0].node_id;
    check_int_eq(flowie_cluster_peer_authority_create(&config, &authority), TURBO_EINVAL);
    check_null(authority);
    fixture.pins[1].node_id = tstr_v_from_cstr("node-b");
    config.max_peers = 0u;
    check_int_eq(flowie_cluster_peer_authority_create(&config, &authority), TURBO_EINVAL);
    check_null(authority);
  }
}
