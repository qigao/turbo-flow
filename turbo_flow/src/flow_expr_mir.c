#include "flow_expr_internal.h"

#include "mir-gen.h"
#include "mir.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TURBO_FLOW_EXPR_ENABLE_JIT
  #define TURBO_FLOW_EXPR_ENABLE_JIT 1
#endif

#if TURBO_FLOW_EXPR_ENABLE_JIT &&                                                                  \
    (defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__) || defined(_M_ARM64) ||        \
     defined(__powerpc64__) || defined(__s390x__) || (defined(__riscv) && __riscv_xlen == 64))
  #define FLOW_EXPR_MIR_JIT_AVAILABLE 1
#else
  #define FLOW_EXPR_MIR_JIT_AVAILABLE 0
#endif

typedef struct flow_expr_mir_frame_s {
  const turbo_flow_expr_t *expr;
  const turbo_flow_expr_eval_context_t *context;
  turbo_flow_expr_value_t values[FLOW_EXPR_MAX_EVAL_DEPTH];
} flow_expr_mir_frame_t;

typedef int64_t (*flow_expr_mir_jit_fn)(flow_expr_mir_frame_t *frame);
typedef int64_t (*flow_expr_mir_eval_node_fn)(flow_expr_mir_frame_t *, int64_t, int64_t, int64_t,
                                              int64_t);
typedef int64_t (*flow_expr_mir_truth_fn)(flow_expr_mir_frame_t *, int64_t);

struct flow_expr_mir_program_s {
  MIR_context_t context;
  MIR_module_t module;
  MIR_item_t function;
  flow_expr_mir_jit_fn jit_fn;
  turbo_mutex_t interp_mutex;
  int interp_mutex_initialized;
  int gen_initialized;
};

typedef struct flow_expr_mir_emitter_s {
  MIR_context_t context;
  MIR_item_t function;
  MIR_item_t eval_proto;
  MIR_item_t eval_import;
  MIR_item_t truth_proto;
  MIR_item_t truth_import;
  MIR_reg_t frame_reg;
  MIR_reg_t status_reg;
  MIR_reg_t truth_reg;
  MIR_label_t truth_fail_label;
  MIR_label_t fail_label;
} flow_expr_mir_emitter_t;

static int flow_expr_mir_set_error(turbo_flow_error_t *error, int code,
                                   const flow_expr_node_t *node, const char *message) {
  if (error) {
    memset(error, 0, sizeof(*error));
    error->code = code;
    if (node) {
      error->line = node->line;
      error->column = node->column;
    }
    snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return code;
}

static turbo_flow_expr_value_type_t flow_expr_public_type(flow_expr_value_type_t type) {
  if (type <= FLOW_EXPR_TYPE_UNRESOLVED || type > FLOW_EXPR_TYPE_STRING) {
    return TURBO_FLOW_EXPR_TYPE_INVALID;
  }
  return (turbo_flow_expr_value_type_t)((int)type - 1);
}

static int flow_expr_mir_i64_add(int64_t left, int64_t right, int64_t *out) {
  if ((right > 0 && left > INT64_MAX - right) || (right < 0 && left < INT64_MIN - right)) {
    return TURBO_ERANGE;
  }
  *out = left + right;
  return TURBO_OK;
}

static int flow_expr_mir_i64_sub(int64_t left, int64_t right, int64_t *out) {
  if ((right < 0 && left > INT64_MAX + right) || (right > 0 && left < INT64_MIN + right)) {
    return TURBO_ERANGE;
  }
  *out = left - right;
  return TURBO_OK;
}

static int flow_expr_mir_i64_mul(int64_t left, int64_t right, int64_t *out) {
  if (left == 0 || right == 0) {
    *out = 0;
    return TURBO_OK;
  }
  if ((left == -1 && right == INT64_MIN) || (right == -1 && left == INT64_MIN)) {
    return TURBO_ERANGE;
  }
  if (left > 0) {
    if ((right > 0 && left > INT64_MAX / right) || (right < 0 && right < INT64_MIN / left)) {
      return TURBO_ERANGE;
    }
  } else if ((right > 0 && left < INT64_MIN / right) || (right < 0 && left < INT64_MAX / right)) {
    return TURBO_ERANGE;
  }
  *out = left * right;
  return TURBO_OK;
}

static int flow_expr_mir_string_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int result = common > 0 ? memcmp(left.data, right.data, common) : 0;
  if (result != 0) return result;
  if (left.len < right.len) return -1;
  if (left.len > right.len) return 1;
  return 0;
}

static int flow_expr_mir_numeric_values(const turbo_flow_expr_value_t *left,
                                        const turbo_flow_expr_value_t *right, double *left_value,
                                        double *right_value) {
  if (!left || !right || !left_value || !right_value ||
      (left->type != TURBO_FLOW_EXPR_TYPE_I64 && left->type != TURBO_FLOW_EXPR_TYPE_F64) ||
      (right->type != TURBO_FLOW_EXPR_TYPE_I64 && right->type != TURBO_FLOW_EXPR_TYPE_F64)) {
    return TURBO_EPROTO;
  }
  *left_value = left->type == TURBO_FLOW_EXPR_TYPE_I64 ? (double)left->as.i64 : left->as.f64;
  *right_value = right->type == TURBO_FLOW_EXPR_TYPE_I64 ? (double)right->as.i64 : right->as.f64;
  return TURBO_OK;
}

static int flow_expr_mir_eval_equality(flow_expr_node_kind_t kind,
                                       const turbo_flow_expr_value_t *left,
                                       const turbo_flow_expr_value_t *right,
                                       turbo_flow_expr_value_t *out) {
  int equal = 0;
  if (left->type == TURBO_FLOW_EXPR_TYPE_NULL || right->type == TURBO_FLOW_EXPR_TYPE_NULL) {
    equal = left->type == TURBO_FLOW_EXPR_TYPE_NULL && right->type == TURBO_FLOW_EXPR_TYPE_NULL;
  } else if ((left->type == TURBO_FLOW_EXPR_TYPE_I64 || left->type == TURBO_FLOW_EXPR_TYPE_F64) &&
             (right->type == TURBO_FLOW_EXPR_TYPE_I64 || right->type == TURBO_FLOW_EXPR_TYPE_F64)) {
    if (left->type == TURBO_FLOW_EXPR_TYPE_I64 && right->type == TURBO_FLOW_EXPR_TYPE_I64) {
      equal = left->as.i64 == right->as.i64;
    } else {
      double l;
      double r;
      if (flow_expr_mir_numeric_values(left, right, &l, &r) != TURBO_OK) return TURBO_EPROTO;
      equal = l == r;
    }
  } else if (left->type != right->type) {
    return TURBO_EPROTO;
  } else {
    switch (left->type) {
    case TURBO_FLOW_EXPR_TYPE_BOOL:
      equal = left->as.boolean == right->as.boolean;
      break;
    case TURBO_FLOW_EXPR_TYPE_STRING:
      equal = flow_expr_mir_string_compare(left->as.string, right->as.string) == 0;
      break;
    default:
      return TURBO_EPROTO;
    }
  }
  out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
  out->as.boolean = kind == FLOW_EXPR_EQ ? equal : !equal;
  return TURBO_OK;
}

static int flow_expr_mir_eval_ordering(flow_expr_node_kind_t kind,
                                       const turbo_flow_expr_value_t *left,
                                       const turbo_flow_expr_value_t *right,
                                       turbo_flow_expr_value_t *out) {
  int comparison = 0;
  if (left->type == TURBO_FLOW_EXPR_TYPE_NULL || right->type == TURBO_FLOW_EXPR_TYPE_NULL) {
    return TURBO_EPROTO;
  }
  if ((left->type == TURBO_FLOW_EXPR_TYPE_I64 || left->type == TURBO_FLOW_EXPR_TYPE_F64) &&
      (right->type == TURBO_FLOW_EXPR_TYPE_I64 || right->type == TURBO_FLOW_EXPR_TYPE_F64)) {
    out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    if (left->type == TURBO_FLOW_EXPR_TYPE_I64 && right->type == TURBO_FLOW_EXPR_TYPE_I64) {
      switch (kind) {
      case FLOW_EXPR_LT:
        out->as.boolean = left->as.i64 < right->as.i64;
        break;
      case FLOW_EXPR_LE:
        out->as.boolean = left->as.i64 <= right->as.i64;
        break;
      case FLOW_EXPR_GT:
        out->as.boolean = left->as.i64 > right->as.i64;
        break;
      case FLOW_EXPR_GE:
        out->as.boolean = left->as.i64 >= right->as.i64;
        break;
      default:
        return TURBO_EINVAL;
      }
      return TURBO_OK;
    } else {
      double l;
      double r;
      if (flow_expr_mir_numeric_values(left, right, &l, &r) != TURBO_OK) return TURBO_EPROTO;
      switch (kind) {
      case FLOW_EXPR_LT:
        out->as.boolean = l < r;
        break;
      case FLOW_EXPR_LE:
        out->as.boolean = l <= r;
        break;
      case FLOW_EXPR_GT:
        out->as.boolean = l > r;
        break;
      case FLOW_EXPR_GE:
        out->as.boolean = l >= r;
        break;
      default:
        return TURBO_EINVAL;
      }
      return TURBO_OK;
    }
  } else if (left->type == TURBO_FLOW_EXPR_TYPE_STRING &&
             right->type == TURBO_FLOW_EXPR_TYPE_STRING) {
    comparison = flow_expr_mir_string_compare(left->as.string, right->as.string);
  } else {
    return TURBO_EPROTO;
  }
  out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
  switch (kind) {
  case FLOW_EXPR_LT:
    out->as.boolean = comparison < 0;
    break;
  case FLOW_EXPR_LE:
    out->as.boolean = comparison <= 0;
    break;
  case FLOW_EXPR_GT:
    out->as.boolean = comparison > 0;
    break;
  case FLOW_EXPR_GE:
    out->as.boolean = comparison >= 0;
    break;
  default:
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

static int flow_expr_mir_eval_arithmetic(const flow_expr_node_t *node,
                                         const turbo_flow_expr_value_t *left,
                                         const turbo_flow_expr_value_t *right,
                                         turbo_flow_expr_value_t *out) {
  if (left->type == TURBO_FLOW_EXPR_TYPE_NULL || right->type == TURBO_FLOW_EXPR_TYPE_NULL) {
    return TURBO_EPROTO;
  }
  if (node->value_type == FLOW_EXPR_TYPE_I64) {
    int64_t result = 0;
    int rc = TURBO_OK;
    if (left->type != TURBO_FLOW_EXPR_TYPE_I64 || right->type != TURBO_FLOW_EXPR_TYPE_I64) {
      return TURBO_EPROTO;
    }
    switch (node->kind) {
    case FLOW_EXPR_ADD:
      rc = flow_expr_mir_i64_add(left->as.i64, right->as.i64, &result);
      break;
    case FLOW_EXPR_SUB:
      rc = flow_expr_mir_i64_sub(left->as.i64, right->as.i64, &result);
      break;
    case FLOW_EXPR_MUL:
      rc = flow_expr_mir_i64_mul(left->as.i64, right->as.i64, &result);
      break;
    case FLOW_EXPR_DIV:
      if (right->as.i64 == 0) return TURBO_EINVAL;
      if (left->as.i64 == INT64_MIN && right->as.i64 == -1) return TURBO_ERANGE;
      result = left->as.i64 / right->as.i64;
      break;
    case FLOW_EXPR_MOD:
      if (right->as.i64 == 0) return TURBO_EINVAL;
      result = left->as.i64 == INT64_MIN && right->as.i64 == -1 ? 0 : left->as.i64 % right->as.i64;
      break;
    default:
      return TURBO_EINVAL;
    }
    if (rc != TURBO_OK) return rc;
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = result;
    return TURBO_OK;
  }
  if (node->value_type == FLOW_EXPR_TYPE_F64) {
    double left_value;
    double right_value;
    if (flow_expr_mir_numeric_values(left, right, &left_value, &right_value) != TURBO_OK) {
      return TURBO_EPROTO;
    }
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    switch (node->kind) {
    case FLOW_EXPR_ADD:
      out->as.f64 = left_value + right_value;
      break;
    case FLOW_EXPR_SUB:
      out->as.f64 = left_value - right_value;
      break;
    case FLOW_EXPR_MUL:
      out->as.f64 = left_value * right_value;
      break;
    case FLOW_EXPR_DIV:
      if (right_value == 0.0) return TURBO_EINVAL;
      out->as.f64 = left_value / right_value;
      break;
    default:
      return TURBO_EINVAL;
    }
    return TURBO_OK;
  }
  return TURBO_EPROTO;
}

static int64_t flow_expr_mir_eval_node(flow_expr_mir_frame_t *frame, int64_t node_index,
                                       int64_t out_slot, int64_t left_slot, int64_t right_slot) {
  const flow_expr_node_t *node;
  turbo_flow_expr_value_t *out;
  turbo_flow_expr_value_t left_value;
  turbo_flow_expr_value_t right_value;
  const turbo_flow_expr_value_t *left = NULL;
  const turbo_flow_expr_value_t *right = NULL;
  if (!frame || !frame->expr || node_index < 0 ||
      (uint64_t)node_index >= flow_expr_ast_node_count(&frame->expr->ast) || out_slot < 0 ||
      out_slot >= FLOW_EXPR_MAX_EVAL_DEPTH) {
    return TURBO_EINVAL;
  }
  node = flow_expr_ast_node_at(&frame->expr->ast, (uint32_t)node_index);
  out = &frame->values[out_slot];
  if (!node) return TURBO_EINVAL;
  if (left_slot >= 0) {
    if (left_slot >= FLOW_EXPR_MAX_EVAL_DEPTH) return TURBO_EINVAL;
    left_value = frame->values[left_slot];
    left = &left_value;
  }
  if (right_slot >= 0) {
    if (right_slot >= FLOW_EXPR_MAX_EVAL_DEPTH) return TURBO_EINVAL;
    right_value = frame->values[right_slot];
    right = &right_value;
  }
  memset(out, 0, sizeof(*out));
  switch (node->kind) {
  case FLOW_EXPR_NULL:
    out->type = TURBO_FLOW_EXPR_TYPE_NULL;
    return TURBO_OK;
  case FLOW_EXPR_BOOL:
    out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    out->as.boolean = node->boolean != 0;
    return TURBO_OK;
  case FLOW_EXPR_I64:
    out->type = TURBO_FLOW_EXPR_TYPE_I64;
    out->as.i64 = node->i64;
    return TURBO_OK;
  case FLOW_EXPR_F64:
    out->type = TURBO_FLOW_EXPR_TYPE_F64;
    out->as.f64 = node->f64;
    return TURBO_OK;
  case FLOW_EXPR_STRING:
    out->type = TURBO_FLOW_EXPR_TYPE_STRING;
    out->as.string = tstr_to_v(node->text);
    return TURBO_OK;
  case FLOW_EXPR_FIELD: {
    turbo_flow_expr_value_type_t expected = flow_expr_public_type(node->value_type);
    int rc;
    if (!frame->context) return TURBO_EINVAL;
    rc = turbo_flow_expr_read_field(
        frame->context, (turbo_flow_expr_field_scope_t)node->field_scope, node->field_id, out);
    if (rc != TURBO_OK) return rc;
    return out->type == TURBO_FLOW_EXPR_TYPE_NULL || out->type == expected ? TURBO_OK
                                                                           : TURBO_EPROTO;
  }
  case FLOW_EXPR_NOT:
    if (!left || left->type != TURBO_FLOW_EXPR_TYPE_BOOL) return TURBO_EPROTO;
    out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    out->as.boolean = !left->as.boolean;
    return TURBO_OK;
  case FLOW_EXPR_POS:
    if (!left ||
        (left->type != TURBO_FLOW_EXPR_TYPE_I64 && left->type != TURBO_FLOW_EXPR_TYPE_F64)) {
      return TURBO_EPROTO;
    }
    *out = *left;
    return TURBO_OK;
  case FLOW_EXPR_NEG:
    if (!left) return TURBO_EPROTO;
    if (left->type == TURBO_FLOW_EXPR_TYPE_I64) {
      if (left->as.i64 == INT64_MIN) return TURBO_ERANGE;
      out->type = TURBO_FLOW_EXPR_TYPE_I64;
      out->as.i64 = -left->as.i64;
      return TURBO_OK;
    }
    if (left->type == TURBO_FLOW_EXPR_TYPE_F64) {
      out->type = TURBO_FLOW_EXPR_TYPE_F64;
      out->as.f64 = -left->as.f64;
      return TURBO_OK;
    }
    return TURBO_EPROTO;
  case FLOW_EXPR_ADD:
  case FLOW_EXPR_SUB:
  case FLOW_EXPR_MUL:
  case FLOW_EXPR_DIV:
  case FLOW_EXPR_MOD:
    return left && right ? flow_expr_mir_eval_arithmetic(node, left, right, out) : TURBO_EINVAL;
  case FLOW_EXPR_EQ:
  case FLOW_EXPR_NE:
    return left && right ? flow_expr_mir_eval_equality(node->kind, left, right, out) : TURBO_EINVAL;
  case FLOW_EXPR_LT:
  case FLOW_EXPR_LE:
  case FLOW_EXPR_GT:
  case FLOW_EXPR_GE:
    return left && right ? flow_expr_mir_eval_ordering(node->kind, left, right, out) : TURBO_EINVAL;
  case FLOW_EXPR_AND:
  case FLOW_EXPR_OR:
    if (!left || !right || left->type != TURBO_FLOW_EXPR_TYPE_BOOL ||
        right->type != TURBO_FLOW_EXPR_TYPE_BOOL) {
      return TURBO_EPROTO;
    }
    out->type = TURBO_FLOW_EXPR_TYPE_BOOL;
    out->as.boolean = node->kind == FLOW_EXPR_AND ? left->as.boolean && right->as.boolean
                                                  : left->as.boolean || right->as.boolean;
    return TURBO_OK;
  default:
    return TURBO_EINVAL;
  }
}

static int64_t flow_expr_mir_truth(flow_expr_mir_frame_t *frame, int64_t slot) {
  if (!frame || slot < 0 || slot >= FLOW_EXPR_MAX_EVAL_DEPTH ||
      frame->values[slot].type != TURBO_FLOW_EXPR_TYPE_BOOL) {
    return TURBO_EPROTO;
  }
  return frame->values[slot].as.boolean != 0;
}

static MIR_op_t flow_expr_mir_reg(flow_expr_mir_emitter_t *emitter, MIR_reg_t reg) {
  return MIR_new_reg_op(emitter->context, reg);
}

static void flow_expr_mir_append(flow_expr_mir_emitter_t *emitter, MIR_insn_t insn) {
  MIR_append_insn(emitter->context, emitter->function, insn);
}

static void flow_expr_mir_emit_status_call(flow_expr_mir_emitter_t *emitter, uint32_t node_index,
                                           uint32_t out_slot, int32_t left_slot,
                                           int32_t right_slot) {
  MIR_op_t ops[8];
  ops[0] = MIR_new_ref_op(emitter->context, emitter->eval_proto);
  ops[1] = MIR_new_ref_op(emitter->context, emitter->eval_import);
  ops[2] = flow_expr_mir_reg(emitter, emitter->status_reg);
  ops[3] = flow_expr_mir_reg(emitter, emitter->frame_reg);
  ops[4] = MIR_new_int_op(emitter->context, (int64_t)node_index);
  ops[5] = MIR_new_int_op(emitter->context, (int64_t)out_slot);
  ops[6] = MIR_new_int_op(emitter->context, (int64_t)left_slot);
  ops[7] = MIR_new_int_op(emitter->context, (int64_t)right_slot);
  flow_expr_mir_append(emitter, MIR_new_insn_arr(emitter->context, MIR_CALL, 8, ops));
  flow_expr_mir_append(emitter,
                       MIR_new_insn(emitter->context, MIR_BNE,
                                    MIR_new_label_op(emitter->context, emitter->fail_label),
                                    flow_expr_mir_reg(emitter, emitter->status_reg),
                                    MIR_new_int_op(emitter->context, TURBO_OK)));
}

static void flow_expr_mir_emit_truth_call(flow_expr_mir_emitter_t *emitter, uint32_t slot) {
  MIR_op_t ops[5];
  ops[0] = MIR_new_ref_op(emitter->context, emitter->truth_proto);
  ops[1] = MIR_new_ref_op(emitter->context, emitter->truth_import);
  ops[2] = flow_expr_mir_reg(emitter, emitter->truth_reg);
  ops[3] = flow_expr_mir_reg(emitter, emitter->frame_reg);
  ops[4] = MIR_new_int_op(emitter->context, (int64_t)slot);
  flow_expr_mir_append(emitter, MIR_new_insn_arr(emitter->context, MIR_CALL, 5, ops));
  flow_expr_mir_append(emitter,
                       MIR_new_insn(emitter->context, MIR_BLT,
                                    MIR_new_label_op(emitter->context, emitter->truth_fail_label),
                                    flow_expr_mir_reg(emitter, emitter->truth_reg),
                                    MIR_new_int_op(emitter->context, 0)));
}

static int flow_expr_mir_emit_node(flow_expr_mir_emitter_t *emitter, const flow_expr_ast_t *ast,
                                   uint32_t node_index, uint32_t slot) {
  const flow_expr_node_t *node = flow_expr_ast_node_at(ast, node_index);
  if (!node || slot >= FLOW_EXPR_MAX_EVAL_DEPTH) return TURBO_EINVAL;
  if (node->kind == FLOW_EXPR_AND || node->kind == FLOW_EXPR_OR) {
    MIR_label_t short_label;
    MIR_label_t done_label;
    int rc = flow_expr_mir_emit_node(emitter, ast, node->left, slot);
    if (rc != TURBO_OK) return rc;
    flow_expr_mir_emit_truth_call(emitter, slot);
    short_label = MIR_new_label(emitter->context);
    done_label = MIR_new_label(emitter->context);
    flow_expr_mir_append(emitter, MIR_new_insn(emitter->context,
                                               node->kind == FLOW_EXPR_AND ? MIR_BEQ : MIR_BNE,
                                               MIR_new_label_op(emitter->context, short_label),
                                               flow_expr_mir_reg(emitter, emitter->truth_reg),
                                               MIR_new_int_op(emitter->context, 0)));
    rc = flow_expr_mir_emit_node(emitter, ast, node->right, slot + 1u);
    if (rc != TURBO_OK) return rc;
    flow_expr_mir_emit_status_call(emitter, node_index, slot, (int32_t)slot, (int32_t)(slot + 1u));
    flow_expr_mir_append(emitter, MIR_new_insn(emitter->context, MIR_JMP,
                                               MIR_new_label_op(emitter->context, done_label)));
    flow_expr_mir_append(emitter, short_label);
    flow_expr_mir_emit_status_call(emitter, node_index, slot, (int32_t)slot, (int32_t)slot);
    flow_expr_mir_append(emitter, done_label);
    return TURBO_OK;
  }
  if (node->left != FLOW_EXPR_INVALID_NODE) {
    int rc = flow_expr_mir_emit_node(emitter, ast, node->left, slot);
    if (rc != TURBO_OK) return rc;
  }
  if (node->right != FLOW_EXPR_INVALID_NODE) {
    int rc = flow_expr_mir_emit_node(emitter, ast, node->right, slot + 1u);
    if (rc != TURBO_OK) return rc;
  }
  flow_expr_mir_emit_status_call(emitter, node_index, slot,
                                 node->left == FLOW_EXPR_INVALID_NODE ? -1 : (int32_t)slot,
                                 node->right == FLOW_EXPR_INVALID_NODE ? -1 : (int32_t)(slot + 1u));
  return TURBO_OK;
}

static int flow_expr_mir_validate_depth(const turbo_flow_expr_t *expr, turbo_flow_error_t *error) {
  uint16_t depth[FLOW_EXPR_MAX_NODES];
  size_t count = flow_expr_ast_node_count(&expr->ast);
  for (size_t i = 0; i < count; ++i) {
    const flow_expr_node_t *node = flow_expr_ast_node_at(&expr->ast, (uint32_t)i);
    uint32_t needed = 1;
    if (!node)
      return flow_expr_mir_set_error(error, TURBO_EINVAL, NULL, "expression AST is invalid");
    if (node->left != FLOW_EXPR_INVALID_NODE) needed = depth[node->left];
    if (node->right != FLOW_EXPR_INVALID_NODE) {
      uint32_t right_needed = 1u + depth[node->right];
      if (right_needed > needed) needed = right_needed;
    }
    if (needed > FLOW_EXPR_MAX_EVAL_DEPTH) {
      return flow_expr_mir_set_error(error, TURBO_ENOSPC, node,
                                     "expression evaluation depth limit exceeded");
    }
    depth[i] = (uint16_t)needed;
  }
  return TURBO_OK;
}

static void *flow_expr_mir_eval_node_address(void) {
  union {
    flow_expr_mir_eval_node_fn function;
    void *address;
  } value;
  value.function = flow_expr_mir_eval_node;
  return value.address;
}

static void *flow_expr_mir_truth_address(void) {
  union {
    flow_expr_mir_truth_fn function;
    void *address;
  } value;
  value.function = flow_expr_mir_truth;
  return value.address;
}

static flow_expr_mir_jit_fn flow_expr_mir_jit_address(void *address) {
  union {
    void *address;
    flow_expr_mir_jit_fn function;
  } value;
  value.address = address;
  return value.function;
}

int flow_expr_mir_compile(turbo_flow_expr_t *expr, turbo_flow_expr_backend_t requested,
                          turbo_flow_error_t *error) {
  flow_expr_mir_program_t *program;
  flow_expr_mir_emitter_t emitter;
  MIR_type_t result_type = MIR_T_I64;
  MIR_var_t function_args[] = {{MIR_T_P, "frame", 0}};
  MIR_var_t eval_args[] = {{MIR_T_P, "frame", 0},
                           {MIR_T_I64, "node", 0},
                           {MIR_T_I64, "out", 0},
                           {MIR_T_I64, "left", 0},
                           {MIR_T_I64, "right", 0}};
  MIR_var_t truth_args[] = {{MIR_T_P, "frame", 0}, {MIR_T_I64, "slot", 0}};
  int rc;
  if (!expr || requested < TURBO_FLOW_EXPR_MIR_INTERP || requested > TURBO_FLOW_EXPR_AUTO) {
    return flow_expr_mir_set_error(error, TURBO_EINVAL, NULL, "expression backend is invalid");
  }
  rc = flow_expr_mir_validate_depth(expr, error);
  if (rc != TURBO_OK) return rc;
#if FLOW_EXPR_MIR_JIT_AVAILABLE
  expr->backend = requested == TURBO_FLOW_EXPR_AUTO ? TURBO_FLOW_EXPR_MIR_JIT : requested;
#else
  if (requested == TURBO_FLOW_EXPR_MIR_JIT) {
    return flow_expr_mir_set_error(error, TURBO_ENOTSUP, NULL,
                                   "MIR JIT expression backend is disabled");
  }
  expr->backend = TURBO_FLOW_EXPR_MIR_INTERP;
#endif
  program = (flow_expr_mir_program_t *)calloc(1, sizeof(*program));
  if (!program) {
    return flow_expr_mir_set_error(error, TURBO_ENOMEM, NULL,
                                   "out of memory creating MIR expression");
  }
  program->context = MIR_init();
  if (!program->context) {
    free(program);
    return flow_expr_mir_set_error(error, TURBO_ENOMEM, NULL, "failed to create MIR context");
  }
  turbo_mutex_init(&program->interp_mutex);
  program->interp_mutex_initialized = 1;
  program->module = MIR_new_module(program->context, "turbo_flow_expr");
  memset(&emitter, 0, sizeof(emitter));
  emitter.context = program->context;
  emitter.eval_import = MIR_new_import(program->context, "tf_expr_eval_node");
  emitter.eval_proto =
      MIR_new_proto_arr(program->context, "tf_expr_eval_node_proto", 1, &result_type, 5, eval_args);
  emitter.truth_import = MIR_new_import(program->context, "tf_expr_truth");
  emitter.truth_proto =
      MIR_new_proto_arr(program->context, "tf_expr_truth_proto", 1, &result_type, 2, truth_args);
  emitter.function =
      MIR_new_func_arr(program->context, "tf_expr_evaluate", 1, &result_type, 1, function_args);
  program->function = emitter.function;
  emitter.frame_reg = MIR_reg(program->context, "frame", emitter.function->u.func);
  emitter.status_reg =
      MIR_new_func_reg(program->context, emitter.function->u.func, MIR_T_I64, "status");
  emitter.truth_reg =
      MIR_new_func_reg(program->context, emitter.function->u.func, MIR_T_I64, "truth");
  emitter.truth_fail_label = MIR_new_label(program->context);
  emitter.fail_label = MIR_new_label(program->context);
  rc = flow_expr_mir_emit_node(&emitter, &expr->ast, expr->ast.root, 0);
  if (rc != TURBO_OK) {
    flow_expr_mir_destroy(program);
    return flow_expr_mir_set_error(error, rc, NULL, "failed to lower expression to MIR");
  }
  flow_expr_mir_append(
      &emitter, MIR_new_ret_insn(program->context, 1, MIR_new_int_op(program->context, TURBO_OK)));
  flow_expr_mir_append(&emitter, emitter.truth_fail_label);
  flow_expr_mir_append(&emitter, MIR_new_insn(program->context, MIR_MOV,
                                              flow_expr_mir_reg(&emitter, emitter.status_reg),
                                              flow_expr_mir_reg(&emitter, emitter.truth_reg)));
  flow_expr_mir_append(&emitter, emitter.fail_label);
  flow_expr_mir_append(&emitter, MIR_new_ret_insn(program->context, 1,
                                                  flow_expr_mir_reg(&emitter, emitter.status_reg)));
  MIR_finish_func(program->context);
  MIR_finish_module(program->context);
  MIR_load_module(program->context, program->module);
  MIR_load_external(program->context, "tf_expr_eval_node", flow_expr_mir_eval_node_address());
  MIR_load_external(program->context, "tf_expr_truth", flow_expr_mir_truth_address());
  if (expr->backend == TURBO_FLOW_EXPR_MIR_JIT) {
    MIR_gen_init(program->context);
    program->gen_initialized = 1;
    MIR_link(program->context, MIR_set_gen_interface, NULL);
    program->jit_fn = flow_expr_mir_jit_address(MIR_gen(program->context, program->function));
    if (!program->jit_fn) {
      flow_expr_mir_destroy(program);
      return flow_expr_mir_set_error(error, TURBO_ENOTSUP, NULL, "MIR JIT generation failed");
    }
  } else {
    flow_expr_mir_frame_t frame;
    turbo_flow_expr_eval_context_t eval_context = TURBO_FLOW_EXPR_EVAL_CONTEXT_INIT;
    turbo_flow_msg_t message;
    MIR_val_t argument;
    MIR_val_t result;

    MIR_link(program->context, MIR_set_interp_interface, NULL);
    memset(&frame, 0, sizeof(frame));
    turbo_flow_msg_init(&message);
    eval_context.message = &message;
    frame.expr = expr;
    frame.context = &eval_context;
    argument.a = &frame;
    result.i = TURBO_EINVAL;
    MIR_interp_arr(program->context, program->function, &result, 1, &argument);
  }
  expr->mir = program;
  return TURBO_OK;
}

int turbo_flow_expr_jit_available(void) { return FLOW_EXPR_MIR_JIT_AVAILABLE; }

void flow_expr_mir_destroy(flow_expr_mir_program_t *program) {
  if (!program) return;
  if (program->gen_initialized) MIR_gen_finish(program->context);
  if (program->context) MIR_finish(program->context);
  if (program->interp_mutex_initialized) turbo_mutex_destroy(&program->interp_mutex);
  free(program);
}

int flow_expr_mir_evaluate(const turbo_flow_expr_t *expr,
                           const turbo_flow_expr_eval_context_t *context,
                           turbo_flow_expr_value_t *out) {
  flow_expr_mir_frame_t frame;
  int64_t status;
  if (!expr || !expr->mir || !context || context->size < sizeof(*context) || !out) {
    return TURBO_EINVAL;
  }
  memset(&frame, 0, sizeof(frame));
  memset(out, 0, sizeof(*out));
  frame.expr = expr;
  frame.context = context;
  if (expr->backend == TURBO_FLOW_EXPR_MIR_JIT) {
    if (!expr->mir->jit_fn) return TURBO_ENOTSUP;
    status = expr->mir->jit_fn(&frame);
  } else {
    MIR_val_t argument;
    MIR_val_t result;
    argument.a = &frame;
    result.i = TURBO_EINVAL;
    turbo_mutex_lock(&expr->mir->interp_mutex);
    MIR_interp_arr(expr->mir->context, expr->mir->function, &result, 1, &argument);
    turbo_mutex_unlock(&expr->mir->interp_mutex);
    status = result.i;
  }
  if (status != TURBO_OK) return (int)status;
  *out = frame.values[0];
  return TURBO_OK;
}
