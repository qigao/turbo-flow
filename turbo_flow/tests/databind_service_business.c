#include "tf_service.service_native.h"

int databind_10_ServiceSdk_4_Calc_3_Add(
    const AddRequest_t *request,
    AddResponse_t *response) {
  if (!request || !response) return -1;
  response->sum = request->left + request->scale;
  return 0;
}

int databind_10_ServiceSdk_4_Calc_5_Other(
    const AddRequest_t *request,
    AddResponse_t *response) {
  if (!request || !response) return -1;
  response->sum = request->left + request->scale + 1u;
  return 0;
}
