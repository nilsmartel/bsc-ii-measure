#include "headers/codecfactory.h"
#include "headers/codecs.h"
#include "headers/util.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/types.h>

#include "c-bridge.h"

extern "C" IntegerCODECPtr CODECFactory_getFromName(const char *name) {
  std::shared_ptr<FastPForLib::IntegerCODEC> codec =
      FastPForLib::CODECFactory::getFromName(name);
  auto ptr = static_cast<void *>(codec.get());

  codec.reset(
      (FastPForLib::IntegerCODEC *)nullptr,
      /* noop, causing pointer to be leaked */ [](const FastPForLib::
                                                      IntegerCODEC *) {});

  return ptr;
}

extern "C" void INTEGERCODEC_destroy(const IntegerCODECPtr p) {
  auto icodec = static_cast<FastPForLib::IntegerCODEC *>(p);

  delete icodec;
}

extern "C" size_t CODEC_encodeArray(const IntegerCODECPtr codec,
                                    const uint32_t *in, size_t length,
                                    uint32_t *out, size_t outlen) {

  size_t nvalue = outlen;

  auto icodec = static_cast<FastPForLib::IntegerCODEC *>(codec);

  icodec->encodeArray(in, length, out, nvalue);

  return nvalue;
}

extern "C" size_t CODEC_decodeArray(const IntegerCODECPtr codec,
                                    const uint32_t *in, const size_t length,
                                    uint32_t *out, size_t outlen) {
  auto icodec = static_cast<FastPForLib::IntegerCODEC *>(codec);

  size_t nvalue = outlen;
  icodec->decodeArray(in, length, out, nvalue);

  return nvalue;
}
