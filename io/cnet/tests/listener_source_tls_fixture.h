#ifndef LISTENER_SOURCE_TLS_FIXTURE_H
#define LISTENER_SOURCE_TLS_FIXTURE_H

#include "tinytest.h"

#include <salts/error_codes.h>

#include <stdlib.h>

static const char LISTENER_SOURCE_TLS_CERTIFICATE[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDODCCAiCgAwIBAgIBAjANBgkqhkiG9w0BAQsFADAcMRowGAYDVQQDDBFUdXJi\n"
    "b0Zsb3cgdGVzdCBDQTAeFw0yNjEwMTEwNTQyMTZaFw0zNjEwMDgwNTQyMTZaMBQx\n"
    "EjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
    "ggEBALTbrrUJWVa1wVvex1+R3zF2zJBEvZ9noJV0Ec1O0IODJirQKa6ZAVUhR3gL\n"
    "YtDcj4GrKGqCUwYtcQYQ7Bk1k7aNEyVR4VuUHGEj0XPMf2nFkDcQUjHVn/EafUMW\n"
    "2hAskG1q/kA2/3NWvUFNHxhZ7UAZy35PZSyRuzEU8IN/Ln50pDqqsgLW0+FeWcCJ\n"
    "pDqPTHRAnGTL0Q2J7x9WrSFLxkSOztzOOuoxnvuVjeBONofFTT0icNiAb2xcXj2d\n"
    "8jFqYlUZG7FpatYSSBxr5xg/uCDxq9pwfxZkfj/evShZv4Erxi/bWBUcN+gn2vnJ\n"
    "8FgGDaS7KcDmmgEHKXjDfLQBiD8CAwEAAaOBjDCBiTAMBgNVHRMBAf8EAjAAMA4G\n"
    "A1UdDwEB/wQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDATAUBgNVHREEDTALggls\n"
    "b2NhbGhvc3QwHQYDVR0OBBYEFHSKGrYW6d59EU5htbnpgVhPLaQiMB8GA1UdIwQY\n"
    "MBaAFEgtLuZV3Ojfv2TpOHJELHhw0wITMA0GCSqGSIb3DQEBCwUAA4IBAQCOyJGm\n"
    "GsUh26aqhwdVzXFqOEmbiHRGaNDuBeKXmbq+UAYkXP/CHaXWD0ztifXHTwXK/UHb\n"
    "smdnAvhWCUyZCNDngTabJLuWnOZJc2MVehb5IAfpKqsp4v3M2C23OgWZdRcdqlXp\n"
    "/RJLE4apLyzhayJFdSCUsd19q6WAtJKfIwDtizJPQAGu6Fz+q0X8K5G58er8WZzs\n"
    "y23rcpE8xWRlvJQoriIsGHNr3rF171m8A20OsgaT+qZ85Qez6SebzoULcCn7zNvZ\n"
    "ZRNcjwx0T/F1h53Wi8qEkcdp7Ir6pWVAeJfOoFHMU4sk8X/g+PznziCfArVOvXn2\n"
    "Ak8zfzUtuUO2RYx7\n"
    "-----END CERTIFICATE-----\n";

/* Test-only trust anchor; the server certificate is a distinct CA-signed leaf. */
static const char LISTENER_SOURCE_TLS_CA[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDLDCCAhSgAwIBAgIUPD9lzVEGVyNf50sUJXPgopgVKmkwDQYJKoZIhvcNAQEL\n"
    "BQAwHDEaMBgGA1UEAwwRVHVyYm9GbG93IHRlc3QgQ0EwHhcNMjYxMDExMDU0MjE1\n"
    "WhcNMzYxMDA4MDU0MjE1WjAcMRowGAYDVQQDDBFUdXJib0Zsb3cgdGVzdCBDQTCC\n"
    "ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAO8fR+PZ9dyOvidt0hmsGC5f\n"
    "0N4W/7nJfwc82g+QRTAs5y6myEf2RmyiSr0vbQnqgnK9s4c3bbbSnmMjzqWDT4uL\n"
    "MB3DSluIiy4KxiWIpF69sXAwpa7XTln3NUSqUNcC0mq04eTldYVM+f0XIBKcK/vs\n"
    "ncGKITBEl/kslkchifMHHGOEo3S9EmeAMXOpsRNJ4+vo0NUg7x39rQnClVsQFCTS\n"
    "KP2eJ/5Jr5qJ4AKP/4XjTxZaDLsDF9i/jizlnLAxPnFD6vMOVV6yQFVm79w8TbAP\n"
    "7fuoacVTFxuvb8DE7x40+Y+CgAUEi+519ZjMxOLSOm/JpL4H5K5lKBYWCW54ylsC\n"
    "AwEAAaNmMGQwHQYDVR0OBBYEFEgtLuZV3Ojfv2TpOHJELHhw0wITMB8GA1UdIwQY\n"
    "MBaAFEgtLuZV3Ojfv2TpOHJELHhw0wITMBIGA1UdEwEB/wQIMAYBAf8CAQAwDgYD\n"
    "VR0PAQH/BAQDAgEGMA0GCSqGSIb3DQEBCwUAA4IBAQDuUAdolrPLIyeig81LVxPv\n"
    "c+go1QOMF5sNxTxxGZM7p1Eifxdk00HDTHMVy3J5toSa4NbB7201/B6zhxA8dY67\n"
    "CaAygEhK+XUojT1Gq96ZkSnEaR4OLeowAuwoOVGaMZwYVhpOEuB9TPAlgGOZIJVh\n"
    "RUVrt0xVEB6J9QbqLFIbFJk22/WwGLx+Q2rKZ8XPVIaP3yxbmvTn68Dm/4u4MxDy\n"
    "56tJI2kZXKQysPCerKp2fRnOcLTvKGqnmyUyp7K0V6DamZsiUVLU+IRnY0ssPqst\n"
    "HdiW9VBvOyJpClfZnieHDUF2wqylzie8aELzl5sLJS0KQn9TI+u2xHKJQKXyGqsV\n"
    "-----END CERTIFICATE-----\n";

static const char LISTENER_SOURCE_TLS_KEY[] =
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

typedef struct listener_source_tls_fixture_s {
  char *cert_path;
  char *key_path;
  char *ca_path;
} listener_source_tls_fixture_t;

static int listener_source_tls_fixture_init(listener_source_tls_fixture_t *fixture) {
  if (!fixture) return SALTS_EINVAL;
  fixture->cert_path = tt_make_temp_file("listener-cert", ".pem");
  fixture->key_path = tt_make_temp_file("listener-key", ".pem");
  fixture->ca_path = tt_make_temp_file("listener-ca", ".pem");
  if (!fixture->cert_path || !fixture->key_path || !fixture->ca_path) return SALTS_ENOMEM;
  if (tt_write_file(fixture->cert_path, LISTENER_SOURCE_TLS_CERTIFICATE,
                    sizeof(LISTENER_SOURCE_TLS_CERTIFICATE) - 1u) != 0 ||
      tt_write_file(fixture->key_path, LISTENER_SOURCE_TLS_KEY,
                    sizeof(LISTENER_SOURCE_TLS_KEY) - 1u) != 0 ||
      tt_write_file(fixture->ca_path, LISTENER_SOURCE_TLS_CA,
                    sizeof(LISTENER_SOURCE_TLS_CA) - 1u) != 0)
    return SALTS_EIO;
  return SALTS_OK;
}

static void listener_source_tls_fixture_destroy(listener_source_tls_fixture_t *fixture) {
  if (!fixture) return;
  if (fixture->cert_path) {
    (void)tt_remove_file(fixture->cert_path);
    free(fixture->cert_path);
  }
  if (fixture->key_path) {
    (void)tt_remove_file(fixture->key_path);
    free(fixture->key_path);
  }
  if (fixture->ca_path) {
    (void)tt_remove_file(fixture->ca_path);
    free(fixture->ca_path);
  }
  fixture->cert_path = NULL;
  fixture->key_path = NULL;
  fixture->ca_path = NULL;
}

#endif /* LISTENER_SOURCE_TLS_FIXTURE_H */
