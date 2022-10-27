#include "./cpp-lib/c-bridge.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

// NOTE:
// It's important to go with 16 byte aligned memory blocks

u_int32_t *alloc_aligned(size_t count) {
  u_int32_t *data = malloc(count * sizeof(u_int32_t) + 16);

  while ((size_t)data & 0b1111) {
    data++;
  }

  return data;
}

int main() {
  size_t N = 1024 * 1024;
  size_t OUTN = N * 1024;

  u_int32_t *data = alloc_aligned(N);
  if (!data)
    printf("malloc failed");

  for (int i = 0; i < N; i += 1)
    data[i] = i + 2;

  // works:
  // varint
  // vbyte
  // maskedvbyte

  // fails:
  // fastbinarypacking16
  // fastbinarypacking32
  const IntegerCODECPtr codec = CODECFactory_getFromName("newpfor");

  u_int32_t *compressed = alloc_aligned(OUTN);
  size_t compressed_length =
      CODEC_encodeArray(codec, data, N, compressed, OUTN);

  printf("%lu bytes compressed\n", compressed_length);

  uint32_t *result = alloc_aligned(N);

  size_t written =
      CODEC_decodeArray(codec, compressed, compressed_length, result, N);

  printf("\nwritten %i bytes\n", (int)written);

  int misses = 0;
  for (int i = 0; i < N; i += 1) {
    if (data[i] != result[i]) {
      misses++;
      printf("# ");
      continue;
    }

    printf(". ");
  }

  printf("\n");
  printf("\n%i of %i failed", misses, (int)N);

  printf("\nwritten %i bytes.\nexpected %i bytes.\nratio: %f\n", (int)written,
         (int)N, (float)written / (float)N);
}
