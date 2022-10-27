#ifndef CODEC_CONNECTOR_H
#define CODEC_CONNECTOR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {

#endif

typedef void *IntegerCODECPtr;

IntegerCODECPtr CODECFactory_getFromName(const char *name);

void INTEGERCODEC_destroy(const IntegerCODECPtr p);

size_t CODEC_encodeArray(const IntegerCODECPtr codec, const uint32_t *in,
                         size_t length, uint32_t *out, size_t outlen);

size_t CODEC_decodeArray(const IntegerCODECPtr codec, const uint32_t *in,
                         size_t length, uint32_t *out, size_t outlen);

#ifdef __cplusplus
}
#endif

#endif
