#include <data_bind.h>
#include <turbo_flow_protocol_inbox_envelope.h>

int main(void) {
  DataBind *codec = NULL;
  DataBindError error = DATA_BIND_ERROR_INIT;
  int status = TurboFlowProtocolInbox_codec_create(&codec, &error);
  if (status != DATA_BIND_OK || !codec) return 1;
  data_bind_free(codec);
  return 0;
}
