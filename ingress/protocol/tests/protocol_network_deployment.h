#ifndef PROTOCOL_NETWORK_DEPLOYMENT_H
#define PROTOCOL_NETWORK_DEPLOYMENT_H

#include <cmeta/interface.h>
#include <stddef.h>

/* Test deployment input is explicit and consumed once, before any provider
 * binding. The module copies filename and owns the optional database runtime. */
#define PROTOCOL_NETWORK_DEPLOYMENT_CONTROL_METHODS(X, I)                                          \
  X(I, R3, int, configure, unsigned, slot, unsigned, receiver_port, const char *, filename)
CMETA_INTERFACE(protocol_network_deployment_control, PROTOCOL_NETWORK_DEPLOYMENT_CONTROL_METHODS);

#define PROTOCOL_NETWORK_DEPLOYMENT_CONTROL_ID "test.protocol.network.deployment"

struct protocol_network_e2e_fixture_s;
typedef struct protocol_network_deployment_lifetime_s {
  size_t cnet_leases, durable_leases, resource_leases, component_scopes;
} protocol_network_deployment_lifetime_t;
int protocol_network_deployment_lifetime(const struct protocol_network_e2e_fixture_s *fixture,
                                         protocol_network_deployment_lifetime_t *out);
int protocol_network_deployment_open(struct protocol_network_e2e_fixture_s *fixture,
                                     const char *cnet_module, const char *durable_module,
                                     int packet_source);
int protocol_network_deployment_close(struct protocol_network_e2e_fixture_s *fixture);

#endif
