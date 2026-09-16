#include <http_client/http.h>
#include <http_server/http.h>

int main(void) {
  chttp_async_client client = {0};
  chttp_server_deferred deferred = CHTTP_SERVER_DEFERRED_INIT;
  return client.impl != 0 || deferred.impl != 0;
}
