#ifndef FLOWIE_TEST_SOCKET_H
#define FLOWIE_TEST_SOCKET_H

#include "turbo_error.h"

#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
typedef SOCKET flowie_test_socket_t;
  #define FLOWIE_TEST_INVALID_SOCKET INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int flowie_test_socket_t;
  #define FLOWIE_TEST_INVALID_SOCKET (-1)
#endif

static void flowie_test_socket_close(flowie_test_socket_t socket_handle) {
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return;
#ifdef _WIN32
  closesocket(socket_handle);
#else
  close(socket_handle);
#endif
}

static unsigned short flowie_test_port(void) {
  struct sockaddr_in address;
#ifdef _WIN32
  SOCKET socket_handle = INVALID_SOCKET;
  int address_size = (int)sizeof(address);
  WSADATA data;
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return 0u;
#else
  int socket_handle = -1;
  socklen_t address_size = (socklen_t)sizeof(address);
#endif
  unsigned short port = 0u;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return 0u;
  if (bind(socket_handle, (struct sockaddr *)&address, sizeof(address)) == 0 &&
      getsockname(socket_handle, (struct sockaddr *)&address, &address_size) == 0) {
    port = ntohs(address.sin_port);
  }
  flowie_test_socket_close(socket_handle);
  return port;
}

static int flowie_test_socket_set_recv_buffer(flowie_test_socket_t socket_handle, size_t bytes);

static flowie_test_socket_t flowie_test_connect_with_recv_buffer(unsigned short port,
                                                                 size_t recv_buffer_bytes) {
  struct sockaddr_in address;
  flowie_test_socket_t socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return FLOWIE_TEST_INVALID_SOCKET;
  if (recv_buffer_bytes != 0u &&
      flowie_test_socket_set_recv_buffer(socket_handle, recv_buffer_bytes) != TURBO_OK) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(socket_handle, (struct sockaddr *)&address, sizeof(address)) != 0) {
    flowie_test_socket_close(socket_handle);
    return FLOWIE_TEST_INVALID_SOCKET;
  }
  return socket_handle;
}

static flowie_test_socket_t flowie_test_connect(unsigned short port) {
  return flowie_test_connect_with_recv_buffer(port, 0u);
}

static int flowie_test_socket_set_recv_buffer(flowie_test_socket_t socket_handle, size_t bytes) {
  int value;
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET || bytes == 0u || bytes > INT_MAX)
    return TURBO_EINVAL;
  value = (int)bytes;
#ifdef _WIN32
  return setsockopt(socket_handle, SOL_SOCKET, SO_RCVBUF, (const char *)&value,
                    (int)sizeof(value)) == 0
             ? TURBO_OK
             : TURBO_EIO;
#else
  return setsockopt(socket_handle, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value)) == 0 ? TURBO_OK
                                                                                      : TURBO_EIO;
#endif
}

static int flowie_test_send(flowie_test_socket_t socket_handle, const uint8_t *data, size_t size) {
  size_t offset = 0u;
  while (offset < size) {
#ifdef _WIN32
    int sent = send(socket_handle, (const char *)data + offset, (int)(size - offset), 0);
#else
    ssize_t sent = send(socket_handle, data + offset, size - offset, 0);
#endif
    if (sent <= 0) return TURBO_EIO;
    offset += (size_t)sent;
  }
  return TURBO_OK;
}

static int flowie_test_recv_exact(flowie_test_socket_t socket_handle, uint8_t *data, size_t size) {
  size_t offset = 0u;
#ifdef _WIN32
  DWORD timeout_ms = 2000u;
  if (setsockopt(socket_handle, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout_ms,
                 (int)sizeof(timeout_ms)) != 0)
    return TURBO_EIO;
#else
  struct timeval timeout = {2, 0};
  if (setsockopt(socket_handle, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
    return TURBO_EIO;
#endif
  while (offset < size) {
#ifdef _WIN32
    int received = recv(socket_handle, (char *)data + offset, (int)(size - offset), 0);
#else
    ssize_t received = recv(socket_handle, data + offset, size - offset, 0);
#endif
    if (received <= 0) return TURBO_EIO;
    offset += (size_t)received;
  }
  return TURBO_OK;
}

static int flowie_test_recv_mqtt5_connack(flowie_test_socket_t socket_handle,
                                          uint8_t session_present, uint16_t receive_maximum,
                                          uint32_t maximum_packet_size) {
  uint8_t expected[] = {0x20u, 0x0bu, 0x00u, 0x00u, 0x08u, 0x21u, 0x00u,
                        0x00u, 0x27u, 0x00u, 0x00u, 0x00u, 0x00u};
  uint8_t received[sizeof(expected)];
  expected[2] = session_present;
  expected[6] = (uint8_t)(receive_maximum >> 8u);
  expected[7] = (uint8_t)receive_maximum;
  expected[9] = (uint8_t)(maximum_packet_size >> 24u);
  expected[10] = (uint8_t)(maximum_packet_size >> 16u);
  expected[11] = (uint8_t)(maximum_packet_size >> 8u);
  expected[12] = (uint8_t)maximum_packet_size;
  if (flowie_test_recv_exact(socket_handle, received, sizeof(received)) != TURBO_OK)
    return TURBO_EPROTO;
  return memcmp(received, expected, sizeof(expected)) == 0 ? TURBO_OK : TURBO_EPROTO;
}

static int flowie_test_socket_readable(flowie_test_socket_t socket_handle, uint32_t timeout_ms) {
  fd_set readers;
  struct timeval timeout;
  int rc;
  if (socket_handle == FLOWIE_TEST_INVALID_SOCKET) return 0;
  FD_ZERO(&readers);
  FD_SET(socket_handle, &readers);
  timeout.tv_sec = (long)(timeout_ms / 1000u);
  timeout.tv_usec = (long)((timeout_ms % 1000u) * 1000u);
#ifdef _WIN32
  rc = select(0, &readers, NULL, NULL, &timeout);
#else
  rc = select(socket_handle + 1, &readers, NULL, NULL, &timeout);
#endif
  return rc > 0 && FD_ISSET(socket_handle, &readers);
}

#endif /* FLOWIE_TEST_SOCKET_H */
