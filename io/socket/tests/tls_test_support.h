#ifndef TURBO_FLOW_TLS_TEST_SUPPORT_H
#define TURBO_FLOW_TLS_TEST_SUPPORT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <unistd.h>
#endif

/* Test-only keypair shared with turbonet/CoroNet/tests/tls_test_support.h. */
static const char s_tls_test_cert_pem[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIC7TCCAdWgAwIBAgIUT4pOT+qAkLpsC1bUF3bYRrTHssQwDQYJKoZIhvcNAQEL\n"
    "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDMyMzA4MDcwMloXDTM2MDMy\n"
    "MDA4MDcwMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\n"
    "AAOCAQ8AMIIBCgKCAQEAtNuutQlZVrXBW97HX5HfMXbMkES9n2eglXQRzU7Qg4Mm\n"
    "KtAprpkBVSFHeAti0NyPgasoaoJTBi1xBhDsGTWTto0TJVHhW5QcYSPRc8x/acWQ\n"
    "NxBSMdWf8Rp9QxbaECyQbWr+QDb/c1a9QU0fGFntQBnLfk9lLJG7MRTwg38ufnSk\n"
    "OqqyAtbT4V5ZwImkOo9MdECcZMvRDYnvH1atIUvGRI7O3M466jGe+5WN4E42h8VN\n"
    "PSJw2IBvbFxePZ3yMWpiVRkbsWlq1hJIHGvnGD+4IPGr2nB/FmR+P969KFm/gSvG\n"
    "L9tYFRw36Cfa+cnwWAYNpLspwOaaAQcpeMN8tAGIPwIDAQABozcwNTAUBgNVHREE\n"
    "DTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFHSKGrYW6d59EU5htbnpgVhPLaQiMA0G\n"
    "CSqGSIb3DQEBCwUAA4IBAQBhIzu8IJ7Pm30nKOfvwgQRKbJDWIBKZz/NYoIP5Ljm\n"
    "fZG+ZZT0BnuCObKTvPwAWERwbIn5cIDNCkVKhQoJc4+KqR9fXptxML+Q3e4lCVo3\n"
    "5jjQpG/r18aZxhfroinp6iCfGcECw/JAXPxC8jOhEgVOPQd/LybM9vO8vraH/dIR\n"
    "YRmIoBvGw+wQMt/PcV0GxYLo6LsYJFs0FuJyiufJ2auNtmW5h8qOdtnagmeo0ehp\n"
    "g5VqPlB3EMa/01r9WmfNQJcBbEF8ONhhPXZCV4uplsXGtN8+Xxrzb3SAYQR9xFry\n"
    "x9YTzT8UMLc26vY1RiF6uwODUJzmSaqmefmapVsWrgi3\n"
    "-----END CERTIFICATE-----\n";

static const char s_tls_test_key_pem[] =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC02661CVlWtcFb\n"
    "3sdfkd8xdsyQRL2fZ6CVdBHNTtCDgyYq0CmumQFVIUd4C2LQ3I+BqyhqglMGLXEG\n"
    "EOwZNZO2jRMlUeFblBxhI9FzzH9pxZA3EFIx1Z/xGn1DFtoQLJBtav5ANv9zVr1B\n"
    "TR8YWe1AGct+T2UskbsxFPCDfy5+dKQ6qrIC1tPhXlnAiaQ6j0x0QJxky9ENie8f\n"
    "Vq0hS8ZEjs7czjrqMZ77lY3gTjaHxU09InDYgG9sXF49nfIxamJVGRuxaWrWEkgc\n"
    "a+cYP7gg8avacH8WZH4/3r0oWb+BK8Yv21gVHDfoJ9r5yfBYBg2kuynA5poBByl4\n"
    "w3y0AYg/AgMBAAECggEAEJkoy4yexQp2mHaLAwZhiX9G/uaQJepeHoPsg6nRZoB0\n"
    "JvG7zD5WlPgyQEjV5NKZM7lVmDt7Cydt0V9e4QwTERSZcToL3gUV0FnNMJIlZLuw\n"
    "+fIRg76rUyFZ5aevPlTDXIdj64N1+6E2SqFH/UrOL1fZXoTthXhKdGgLkBtCqnA6\n"
    "DlHQX3lehrnV+MG5fTxPc8lro/s4UVAoBMhc4dP5U1W5Xt5c6RsdcWYytidRYj8t\n"
    "XMkyjST/F2NV80+8WGp/YFE0dHyxGWvLGNmkOUuI4EMwzzSadsIM+PQO/YP1KwHA\n"
    "0DYHuEFvPCLjPsD+7IUnZgifQe45/FJoJMp5hSmzgQKBgQD7XEl2mLR3iqup2dF+\n"
    "PD3zA2J48jdiJdbK7vRLXpdV5WP2/s90GZFKLadg7UWmx9zWkC4B92atNJV0/+8o\n"
    "wE4Zd8PG62QZ3o1T4QpYMem9PAq5OxqwYBxMZ2Y5Mf+54Gp0SXB+AbXPlYI/LIwP\n"
    "i/2Iq+bAjGmuGuloJNWD3Wl3DwKBgQC4MkMYvf5aSqbL8GE5ndKY06HzbxwcMoh3\n"
    "Hia5LRMw5dG3J2JwdruiE4V3gQyqz0NzYrrqqkyYxh3aJW934qj6JVMVw/xWx2n5\n"
    "xB4X4hcCKrO2piROmOuXBEt1T36C+fShNb8g+RNY0edoiw+OKTa3rzlQhggTkoGs\n"
    "Iy7oyxtb0QKBgGKkgfP304LCOcHrSCppC8qtflyGebObs+Jpyhc15OABqKxKrTEb\n"
    "w4e/yNrh4p6j+od9h4CgDXxVkX2b3sg4R6348SzEPcFlNENBomSgGeF4iaDNkBi9\n"
    "bv2Q6m3xsDDK4BwIogvhMe9n9fhCzChhwLp8846GzAZWa1jCc8RPBM+DAoGAQxRy\n"
    "4QDYL5O+OMka7zutpWB1O008hHxWvGKroYZr1cPsYvIh5GkpHfZUBdhmf5Ips0zC\n"
    "W5GXgY+s8XPuq09NUIPlRSjxrbzDuGUWvIXm8TAR8LOCx2jja0TyIg/IN/TFhSwo\n"
    "pd5vkEopJyZ1jMUvmydiDRQyvsX9GW5auAa3uPECgYBxuBJ6Vji7pxlqjG3aB0je\n"
    "+JexLyzdckU7EKTxpTSU1o/p17QpT26KF+DPMc2kg+PBK+Sjm0m4Uxdzq/OXNMMA\n"
    "zhR6Vjo1nPWsKgzK03hGzaJVMkHekgCidY9R+MZEeDAhHDIia9XyAS1qCoGAJ6WC\n"
    "oYB4EuDLFhurWiLO+diuMg==\n"
    "-----END PRIVATE KEY-----\n";

static void tls_test_remove_file(const char *path) {
  if (path && path[0] != '\0') (void)remove(path);
}

static int tls_test_write_temp_file(char *path,
                                    size_t path_len,
                                    const char *tag,
                                    const char *contents) {
  FILE *fp;
  size_t contents_len;
  int write_ok;

  if (!path || path_len == 0 || !tag || !contents) return -1;
  path[0] = '\0';

#ifdef _WIN32
  {
    char temp_dir[MAX_PATH];
    char temp_file[MAX_PATH];
    DWORD dir_len = GetTempPathA((DWORD)sizeof(temp_dir), temp_dir);
    if (dir_len == 0 || dir_len >= sizeof(temp_dir)) return -1;
    if (GetTempFileNameA(temp_dir, tag, 0, temp_file) == 0) return -1;
    if (strlen(temp_file) + 1 > path_len) {
      (void)DeleteFileA(temp_file);
      return -1;
    }
    memcpy(path, temp_file, strlen(temp_file) + 1);
  }
#else
  {
    char temp_file[128];
    int written = snprintf(temp_file, sizeof(temp_file), "/tmp/turbo_flow_tls_%s_XXXXXX", tag);
    int fd;
    if (written < 0 || (size_t)written >= sizeof(temp_file)) return -1;
    fd = mkstemp(temp_file);
    if (fd < 0) return -1;
    (void)close(fd);
    if (strlen(temp_file) + 1 > path_len) {
      (void)unlink(temp_file);
      return -1;
    }
    memcpy(path, temp_file, strlen(temp_file) + 1);
  }
#endif

  fp = fopen(path, "wb");
  if (!fp) {
    tls_test_remove_file(path);
    path[0] = '\0';
    return -1;
  }
  contents_len = strlen(contents);
  write_ok = fwrite(contents, 1, contents_len, fp) == contents_len;
  if (fclose(fp) != 0) write_ok = 0;
  if (!write_ok) {
    tls_test_remove_file(path);
    path[0] = '\0';
    return -1;
  }
  return 0;
}

static int tls_test_write_ca_file(char *path, size_t path_len) {
  return tls_test_write_temp_file(path, path_len, "ca", s_tls_test_cert_pem);
}

static int tls_test_write_server_files(char *cert_path,
                                       size_t cert_path_len,
                                       char *key_path,
                                       size_t key_path_len) {
  if (tls_test_write_temp_file(cert_path, cert_path_len, "crt", s_tls_test_cert_pem) != 0) {
    return -1;
  }
  if (tls_test_write_temp_file(key_path, key_path_len, "key", s_tls_test_key_pem) != 0) {
    tls_test_remove_file(cert_path);
    cert_path[0] = '\0';
    return -1;
  }
  return 0;
}

static int tls_test_set_ca_file_env(const char *path) {
  if (!path || path[0] == '\0') return -1;
#ifdef _WIN32
  if (_putenv_s("TURBONET_TLS_CA_FILE", path) != 0) return -1;
  if (_putenv_s("TURBONET_TLS_CA_PATH", "") != 0) return -1;
#else
  if (setenv("TURBONET_TLS_CA_FILE", path, 1) != 0) return -1;
  if (unsetenv("TURBONET_TLS_CA_PATH") != 0) return -1;
#endif
  return 0;
}

static int tls_test_set_server_env(const char *cert_path, const char *key_path) {
  if (!cert_path || cert_path[0] == '\0' || !key_path || key_path[0] == '\0') return -1;
#ifdef _WIN32
  if (_putenv_s("TURBONET_TLS_CERT_FILE", cert_path) != 0) return -1;
  if (_putenv_s("TURBONET_TLS_KEY_FILE", key_path) != 0) return -1;
#else
  if (setenv("TURBONET_TLS_CERT_FILE", cert_path, 1) != 0) return -1;
  if (setenv("TURBONET_TLS_KEY_FILE", key_path, 1) != 0) return -1;
#endif
  return 0;
}

static void tls_test_clear_ca_env(void) {
#ifdef _WIN32
  (void)_putenv_s("TURBONET_TLS_CA_FILE", "");
  (void)_putenv_s("TURBONET_TLS_CA_PATH", "");
#else
  (void)unsetenv("TURBONET_TLS_CA_FILE");
  (void)unsetenv("TURBONET_TLS_CA_PATH");
#endif
}

static void tls_test_clear_server_env(void) {
#ifdef _WIN32
  (void)_putenv_s("TURBONET_TLS_CERT_FILE", "");
  (void)_putenv_s("TURBONET_TLS_KEY_FILE", "");
#else
  (void)unsetenv("TURBONET_TLS_CERT_FILE");
  (void)unsetenv("TURBONET_TLS_KEY_FILE");
#endif
}

#endif /* TURBO_FLOW_TLS_TEST_SUPPORT_H */
