#ifndef TURBO_FLOW_RULESFORGE_PLUGIN_H
#define TURBO_FLOW_RULESFORGE_PLUGIN_H

#include "turbo_flow.h"

#include <cmeta/data.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TURBO_FLOW_RULESFORGE_PLUGIN_ID "rulesforge.provider"
#define TURBO_FLOW_RULESFORGE_PLUGIN_VERSION "1.0.0"
#define TURBO_FLOW_RULESFORGE_OPERATION "rulesforge.apply"
#define TURBO_FLOW_RULESFORGE_RESOURCE_KIND "rulesforge.knowledge-base"
#define TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID "rulesforge.Applicant.data"
#define TURBO_FLOW_RULESFORGE_OUTPUT_SCHEMA_ID "rulesforge.Decision.data"
#define TURBO_FLOW_RULESFORGE_SCHEMA_VERSION 1u

Struct(turbo_flow_rulesforge_applicant, (int, age));
Struct(turbo_flow_rulesforge_decision, (int, matched), (int, fired));

static const cmeta_type_desc turbo_flow_rulesforge_applicant_type = {
    "turbo_flow_rulesforge_applicant", sizeof(turbo_flow_rulesforge_applicant),
    _Alignof(turbo_flow_rulesforge_applicant), CMETA_T_OBJECT, NULL, NULL, NULL};

static const cmeta_data_field_desc turbo_flow_rulesforge_applicant_fields[] = {
    {"rulesforge.Applicant.age", "age", offsetof(turbo_flow_rulesforge_applicant, age),
     &cmeta_data_int}};

static const cmeta_data_struct_shape turbo_flow_rulesforge_applicant_shape = {
    StructMeta(turbo_flow_rulesforge_applicant), turbo_flow_rulesforge_applicant_fields, 1u};

static const cmeta_data_desc turbo_flow_rulesforge_applicant_data = {
    sizeof(cmeta_data_desc), CMETA_DATA_DESC_ABI_VERSION, TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID,
    "Applicant", CMETA_DATA_STRUCT, &turbo_flow_rulesforge_applicant_type,
    &turbo_flow_rulesforge_applicant_shape, NULL, NULL, NULL};

static const cmeta_type_desc turbo_flow_rulesforge_decision_type = {
    "turbo_flow_rulesforge_decision", sizeof(turbo_flow_rulesforge_decision),
    _Alignof(turbo_flow_rulesforge_decision), CMETA_T_OBJECT, NULL, NULL, NULL};

static const cmeta_data_field_desc turbo_flow_rulesforge_decision_fields[] = {
    {"rulesforge.Decision.matched", "matched", offsetof(turbo_flow_rulesforge_decision, matched),
     &cmeta_data_int},
    {"rulesforge.Decision.fired", "fired", offsetof(turbo_flow_rulesforge_decision, fired),
     &cmeta_data_int}};

static const cmeta_data_struct_shape turbo_flow_rulesforge_decision_shape = {
    StructMeta(turbo_flow_rulesforge_decision), turbo_flow_rulesforge_decision_fields, 2u};

static const cmeta_data_desc turbo_flow_rulesforge_decision_data = {
    sizeof(cmeta_data_desc), CMETA_DATA_DESC_ABI_VERSION, TURBO_FLOW_RULESFORGE_OUTPUT_SCHEMA_ID,
    "Decision", CMETA_DATA_STRUCT, &turbo_flow_rulesforge_decision_type,
    &turbo_flow_rulesforge_decision_shape, NULL, NULL, NULL};

static const turbo_flow_data_schema_t turbo_flow_rulesforge_applicant_schema = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
    TURBO_FLOW_RULESFORGE_INPUT_SCHEMA_ID, "Applicant", "turbo_flow_rulesforge_applicant",
    7301u, TURBO_FLOW_RULESFORGE_SCHEMA_VERSION, NULL};

static const turbo_flow_data_schema_t turbo_flow_rulesforge_decision_schema = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_DATA, TURBO_FLOW_DATA_ENCODING_OPAQUE,
    TURBO_FLOW_RULESFORGE_OUTPUT_SCHEMA_ID, "Decision", "turbo_flow_rulesforge_decision",
    7302u, TURBO_FLOW_RULESFORGE_SCHEMA_VERSION, NULL};

#ifdef __cplusplus
}
#endif

#endif
