#ifndef TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H
#define TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H

#include "flow_internal.h"

#include <cserde/reader.h>
#include <data_bind_message_plan.h>
#include <data_bind_native.h>
#include <data_bind_native_binding.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flow_provider_config_reader_s {
  cserde_reader reader;
  const vec_t *literals;
  size_t token_index;
} flow_provider_config_reader_t;

enum {
  FLOW_PROVIDER_CONFIG_WORKSPACE_BYTES = 65536u,
  FLOW_PROVIDER_CONFIG_MAX_DEPTH = 32u,
  FLOW_PROVIDER_CONFIG_MAX_ITEMS = 1024u,
  FLOW_PROVIDER_CONFIG_MAX_OWNED_BYTES = 65536u
};

typedef DataBindStatus (*flow_provider_config_codec_factory_fn)(
    DataBind **out, DataBindError *error);

typedef struct flow_provider_typed_config_s {
  const DataBindMessageNativeArtifact *artifact;
  DataBindNativeTypeBinding native;
  DataBindMessagePlan *plan;
  void *storage_allocation;
  void *value;
  size_t value_bytes;
  void *workspace;
  size_t workspace_bytes;
  DataBindNativeOptions options;
  int value_live;
} flow_provider_typed_config_t;

typedef struct flow_provider_typed_config_view_s {
  const char *type_name;
  const cmeta_data_desc *data;
  const void *value;
  size_t value_bytes;
} flow_provider_typed_config_view_t;

int flow_provider_config_literals_init(vec_t *literals);
void flow_provider_config_literals_destroy(vec_t *literals);
int flow_provider_config_literals_copy(vec_t *destination, const vec_t *source);

int flow_provider_config_reader_init(
    flow_provider_config_reader_t *config_reader,
    const flow_stage_plan_impl_t *stage);

int flow_provider_typed_config_bind(
    const flow_stage_plan_impl_t *stage,
    flow_provider_config_codec_factory_fn codec_factory,
    const DataBindMessageNativeArtifact *artifact,
    flow_provider_typed_config_t *out,
    DataBindMessagePlanDiagnostic *diagnostic);

int flow_provider_typed_config_view(
    const flow_provider_typed_config_t *config,
    flow_provider_typed_config_view_t *out);

int flow_provider_typed_config_destroy(flow_provider_typed_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_PROVIDER_CONFIG_INTERNAL_H */
