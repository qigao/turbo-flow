#ifndef TURBO_FLOW_TLS_TEST_PKI_H
#define TURBO_FLOW_TLS_TEST_PKI_H

#include "tls_test_support.h"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#define TLS_TEST_PKI_RSA_BITS 2048
#define TLS_TEST_PKI_VALID_SECONDS 31536000L

static EVP_PKEY *tls_test_pki_generate_key(void) {
  EVP_PKEY_CTX *context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
  EVP_PKEY *key = NULL;
  if (!context || EVP_PKEY_keygen_init(context) <= 0 ||
      EVP_PKEY_CTX_set_rsa_keygen_bits(context, TLS_TEST_PKI_RSA_BITS) <= 0 ||
      EVP_PKEY_keygen(context, &key) <= 0) {
    EVP_PKEY_free(key);
    key = NULL;
  }
  EVP_PKEY_CTX_free(context);
  return key;
}

static int tls_test_pki_add_extension(X509 *certificate, int nid, const char *value) {
  X509V3_CTX context;
  X509_EXTENSION *extension;
  if (!certificate || !value) return -1;
  X509V3_set_ctx(&context, certificate, certificate, NULL, NULL, 0);
  extension = X509V3_EXT_nconf_nid(NULL, &context, nid, value);
  if (!extension) return -1;
  if (X509_add_ext(certificate, extension, -1) != 1) {
    X509_EXTENSION_free(extension);
    return -1;
  }
  X509_EXTENSION_free(extension);
  return 0;
}

static X509 *tls_test_pki_generate_self_signed(EVP_PKEY *key, const char *common_name, int is_ca) {
  X509 *certificate = X509_new();
  X509_NAME *subject;
  if (!certificate || !key || !common_name || common_name[0] == '\0') goto fail;
  if (X509_set_version(certificate, 2) != 1 ||
      ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1) != 1 ||
      !X509_gmtime_adj(X509_get_notBefore(certificate), -300) ||
      !X509_gmtime_adj(X509_get_notAfter(certificate), TLS_TEST_PKI_VALID_SECONDS))
    goto fail;
  subject = X509_get_subject_name(certificate);
  if (!subject || X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                             (const unsigned char *)common_name, -1, -1, 0) != 1 ||
      X509_set_issuer_name(certificate, subject) != 1 ||
      X509_set_pubkey(certificate, key) != 1)
    goto fail;
  if (is_ca) {
    if (tls_test_pki_add_extension(certificate, NID_basic_constraints, "critical,CA:TRUE") != 0 ||
        tls_test_pki_add_extension(certificate, NID_key_usage,
                                   "critical,keyCertSign,cRLSign") != 0)
      goto fail;
  } else {
    if (tls_test_pki_add_extension(certificate, NID_basic_constraints, "critical,CA:FALSE") != 0 ||
        tls_test_pki_add_extension(certificate, NID_key_usage,
                                   "critical,digitalSignature,keyEncipherment") != 0 ||
        tls_test_pki_add_extension(certificate, NID_ext_key_usage, "clientAuth") != 0)
      goto fail;
  }
  if (X509_sign(certificate, key, EVP_sha256()) <= 0) goto fail;
  return certificate;

fail:
  X509_free(certificate);
  return NULL;
}

static int tls_test_pki_write_key(const char *path, EVP_PKEY *key) {
  BIO *file = NULL;
  int rc = -1;
  if (!path || !key) return -1;
  file = BIO_new_file(path, "wb");
  if (file && PEM_write_bio_PrivateKey(file, key, NULL, NULL, 0, NULL, NULL) == 1) rc = 0;
  BIO_free(file);
  return rc;
}

static int tls_test_pki_write_certificate(const char *path, X509 *certificate) {
  BIO *file = NULL;
  int rc = -1;
  if (!path || !certificate) return -1;
  file = BIO_new_file(path, "wb");
  if (file && PEM_write_bio_X509(file, certificate) == 1) rc = 0;
  BIO_free(file);
  return rc;
}

static int tls_test_write_self_signed_files(char *cert_path, size_t cert_path_len,
                                            char *key_path, size_t key_path_len,
                                            const char *common_name, int is_ca) {
  EVP_PKEY *key = NULL;
  X509 *certificate = NULL;
  int rc = -1;
  if (!cert_path || cert_path_len == 0u || !key_path || key_path_len == 0u) return -1;
  cert_path[0] = '\0';
  key_path[0] = '\0';
  if (tls_test_write_temp_file(cert_path, cert_path_len, "crt", "") != 0 ||
      tls_test_write_temp_file(key_path, key_path_len, "key", "") != 0)
    goto done;
  key = tls_test_pki_generate_key();
  certificate = tls_test_pki_generate_self_signed(key, common_name, is_ca);
  if (!key || !certificate || tls_test_pki_write_certificate(cert_path, certificate) != 0 ||
      tls_test_pki_write_key(key_path, key) != 0)
    goto done;
  rc = 0;

done:
  X509_free(certificate);
  EVP_PKEY_free(key);
  if (rc != 0) {
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    key_path[0] = '\0';
    cert_path[0] = '\0';
  }
  return rc;
}

static int tls_test_write_unrelated_key_file(char *key_path, size_t key_path_len) {
  EVP_PKEY *key = NULL;
  int rc = -1;
  if (!key_path || key_path_len == 0u) return -1;
  key_path[0] = '\0';
  if (tls_test_write_temp_file(key_path, key_path_len, "key", "") != 0) return -1;
  key = tls_test_pki_generate_key();
  if (key && tls_test_pki_write_key(key_path, key) == 0) rc = 0;
  EVP_PKEY_free(key);
  if (rc != 0) {
    tls_test_remove_file(key_path);
    key_path[0] = '\0';
  }
  return rc;
}

static int tls_test_write_expired_server_files(char *cert_path, size_t cert_path_len,
                                               char *key_path, size_t key_path_len) {
  EVP_PKEY *key = NULL;
  X509 *certificate = NULL;
  int rc = -1;
  if (!cert_path || cert_path_len == 0u || !key_path || key_path_len == 0u) return -1;
  cert_path[0] = '\0';
  key_path[0] = '\0';
  if (tls_test_write_temp_file(cert_path, cert_path_len, "expired-crt", "") != 0 ||
      tls_test_write_temp_file(key_path, key_path_len, "expired-key", "") != 0)
    goto done;
  key = tls_test_pki_generate_key();
  certificate = tls_test_pki_generate_self_signed(key, "localhost", 1);
  if (!key || !certificate ||
      !X509_gmtime_adj(X509_get_notBefore(certificate), -7200L) ||
      !X509_gmtime_adj(X509_get_notAfter(certificate), -3600L) ||
      X509_sign(certificate, key, EVP_sha256()) <= 0 ||
      tls_test_pki_write_certificate(cert_path, certificate) != 0 ||
      tls_test_pki_write_key(key_path, key) != 0)
    goto done;
  rc = 0;
done:
  X509_free(certificate);
  EVP_PKEY_free(key);
  if (rc != 0) {
    tls_test_remove_file(key_path);
    tls_test_remove_file(cert_path);
    key_path[0] = '\0';
    cert_path[0] = '\0';
  }
  return rc;
}

#endif /* TURBO_FLOW_TLS_TEST_PKI_H */
