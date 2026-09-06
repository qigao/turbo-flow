#ifndef STREAM_SOURCE_PIPE_FIXTURE_H
#define STREAM_SOURCE_PIPE_FIXTURE_H

#include "tinytest.h"

#include <salts/error_codes.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
  // clang-format off
#  include <winsock2.h>
#  include <windows.h>
// clang-format on

typedef struct stream_source_pipe_fixture_s {
  char name[192];
  HANDLE server;
  HANDLE connect_event;
  OVERLAPPED connect_operation;
  int connect_pending;
} stream_source_pipe_fixture_t;

enum { STREAM_SOURCE_PIPE_FIXTURE_TIMEOUT_MS = 5000 };

static void stream_source_pipe_fixture_reset(stream_source_pipe_fixture_t *fixture) {
  if (!fixture) return;
  memset(fixture, 0, sizeof(*fixture));
  fixture->server = INVALID_HANDLE_VALUE;
}

static int stream_source_pipe_fixture_start(stream_source_pipe_fixture_t *fixture) {
  static LONG sequence = 0;
  char native_name[sizeof(fixture->name) + 10u];
  int length;
  DWORD error;
  if (!fixture) return SALTS_EINVAL;
  stream_source_pipe_fixture_reset(fixture);
  length = snprintf(fixture->name, sizeof(fixture->name), "turbo-flow-%lu-%ld",
                    GetCurrentProcessId(), InterlockedIncrement(&sequence));
  if (length < 0 || (size_t)length >= sizeof(fixture->name)) return SALTS_ERANGE;
  length = snprintf(native_name, sizeof(native_name), "\\\\.\\pipe\\%s", fixture->name);
  if (length < 0 || (size_t)length >= sizeof(native_name)) return SALTS_ERANGE;
  fixture->server =
      CreateNamedPipeA(native_name, PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
                       PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT, 1u, 4096u, 4096u, 0u, NULL);
  if (fixture->server == INVALID_HANDLE_VALUE) return -(int)GetLastError();
  fixture->connect_event = CreateEventA(NULL, TRUE, FALSE, NULL);
  if (!fixture->connect_event) {
    error = GetLastError();
    (void)CloseHandle(fixture->server);
    stream_source_pipe_fixture_reset(fixture);
    return -(int)error;
  }
  fixture->connect_operation.hEvent = fixture->connect_event;
  if (!ConnectNamedPipe(fixture->server, &fixture->connect_operation)) {
    error = GetLastError();
    if (error == ERROR_IO_PENDING) {
      fixture->connect_pending = 1;
    } else if (error != ERROR_PIPE_CONNECTED) {
      (void)CloseHandle(fixture->connect_event);
      (void)CloseHandle(fixture->server);
      stream_source_pipe_fixture_reset(fixture);
      return -(int)error;
    }
  }
  return SALTS_OK;
}

static int stream_source_pipe_fixture_finish(stream_source_pipe_fixture_t *fixture) {
  DWORD transferred = 0u;
  DWORD wait_status;
  if (!fixture || fixture->server == INVALID_HANDLE_VALUE) return SALTS_EINVAL;
  if (fixture->connect_pending) {
    wait_status = WaitForSingleObject(fixture->connect_event, STREAM_SOURCE_PIPE_FIXTURE_TIMEOUT_MS);
    if (wait_status == WAIT_TIMEOUT) return SALTS_ETIMEDOUT;
    if (wait_status != WAIT_OBJECT_0) return -(int)GetLastError();
    if (!GetOverlappedResult(fixture->server, &fixture->connect_operation, &transferred, FALSE))
      return -(int)GetLastError();
  }
  fixture->connect_pending = 0;
  return SALTS_OK;
}

static int stream_source_pipe_fixture_write(stream_source_pipe_fixture_t *fixture, const void *data,
                                            size_t size) {
  OVERLAPPED operation = {0};
  HANDLE event;
  DWORD transferred = 0u;
  DWORD error;
  DWORD wait_status;
  BOOL accepted;
  if (!fixture || fixture->server == INVALID_HANDLE_VALUE || !data || size == 0u ||
      size > UINT32_MAX) {
    return SALTS_EINVAL;
  }
  event = CreateEventA(NULL, TRUE, FALSE, NULL);
  if (!event) return -(int)GetLastError();
  operation.hEvent = event;
  accepted = WriteFile(fixture->server, data, (DWORD)size, NULL, &operation);
  if (!accepted) {
    error = GetLastError();
    if (error != ERROR_IO_PENDING) {
      (void)CloseHandle(event);
      return -(int)error;
    }
  }
  wait_status = WaitForSingleObject(event, STREAM_SOURCE_PIPE_FIXTURE_TIMEOUT_MS);
  if (wait_status == WAIT_TIMEOUT) {
    (void)CancelIoEx(fixture->server, &operation);
    (void)CloseHandle(event);
    return SALTS_ETIMEDOUT;
  }
  if (wait_status != WAIT_OBJECT_0 ||
      !GetOverlappedResult(fixture->server, &operation, &transferred, FALSE)) {
    error = GetLastError();
    (void)CloseHandle(event);
    return -(int)error;
  }
  (void)CloseHandle(event);
  return transferred == size ? SALTS_OK : SALTS_EIO;
}

static void stream_source_pipe_fixture_close(stream_source_pipe_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->connect_event) (void)CloseHandle(fixture->connect_event);
  if (fixture->server != INVALID_HANDLE_VALUE) (void)CloseHandle(fixture->server);
  stream_source_pipe_fixture_reset(fixture);
}

#else

  #include <errno.h>
  #include <fcntl.h>
  #include <sys/stat.h>
  #include <unistd.h>

typedef struct stream_source_pipe_fixture_s {
  char name[512];
  char read_name[516];
  char write_name[516];
  char *directory;
  int peer_read;
  int peer_write;
} stream_source_pipe_fixture_t;

static void stream_source_pipe_fixture_reset(stream_source_pipe_fixture_t *fixture) {
  if (!fixture) return;
  memset(fixture, 0, sizeof(*fixture));
  fixture->peer_read = -1;
  fixture->peer_write = -1;
}

static int stream_source_pipe_fixture_start(stream_source_pipe_fixture_t *fixture) {
  int dummy_read;
  int length;
  if (!fixture) return SALTS_EINVAL;
  stream_source_pipe_fixture_reset(fixture);
  fixture->directory = tt_make_temp_dir("turbo-flow-pipe");
  if (!fixture->directory) return SALTS_EIO;
  length = snprintf(fixture->name, sizeof(fixture->name), "%s/endpoint", fixture->directory);
  if (length < 0 || (size_t)length >= sizeof(fixture->name)) return SALTS_ERANGE;
  length = snprintf(fixture->read_name, sizeof(fixture->read_name), "%s.rx", fixture->name);
  if (length < 0 || (size_t)length >= sizeof(fixture->read_name)) return SALTS_ERANGE;
  length = snprintf(fixture->write_name, sizeof(fixture->write_name), "%s.tx", fixture->name);
  if (length < 0 || (size_t)length >= sizeof(fixture->write_name)) return SALTS_ERANGE;
  if (mkfifo(fixture->read_name, 0600) != 0 || mkfifo(fixture->write_name, 0600) != 0)
    return -errno;
  fixture->peer_read = open(fixture->write_name, O_RDONLY | O_NONBLOCK);
  if (fixture->peer_read < 0) return -errno;
  dummy_read = open(fixture->read_name, O_RDONLY | O_NONBLOCK);
  if (dummy_read < 0) return -errno;
  fixture->peer_write = open(fixture->read_name, O_WRONLY | O_NONBLOCK);
  (void)close(dummy_read);
  return fixture->peer_write >= 0 ? SALTS_OK : -errno;
}

static int stream_source_pipe_fixture_finish(stream_source_pipe_fixture_t *fixture) {
  return fixture && fixture->peer_read >= 0 && fixture->peer_write >= 0 ? SALTS_OK : SALTS_EINVAL;
}

static int stream_source_pipe_fixture_write(stream_source_pipe_fixture_t *fixture, const void *data,
                                            size_t size) {
  ssize_t written;
  if (!fixture || fixture->peer_write < 0 || !data || size == 0u) return SALTS_EINVAL;
  written = write(fixture->peer_write, data, size);
  return written == (ssize_t)size ? SALTS_OK : (written < 0 ? -errno : SALTS_EIO);
}

static void stream_source_pipe_fixture_close(stream_source_pipe_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->peer_read >= 0) (void)close(fixture->peer_read);
  if (fixture->peer_write >= 0) (void)close(fixture->peer_write);
  if (fixture->read_name[0]) (void)unlink(fixture->read_name);
  if (fixture->write_name[0]) (void)unlink(fixture->write_name);
  if (fixture->directory) {
    (void)tt_remove_tree(fixture->directory);
    free(fixture->directory);
  }
  stream_source_pipe_fixture_reset(fixture);
}

#endif

#endif /* STREAM_SOURCE_PIPE_FIXTURE_H */
