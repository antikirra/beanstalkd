#define _GNU_SOURCE
#include "dat.h"
#include <stdint.h>
#include <nmmintrin.h>

// CRC32C (Castagnoli, polynomial 0x1EDC6F41 reflected as 0x82F63B78),
// hardware-accelerated via the Intel SSE4.2 CRC32 instruction.
// SSE4.2 has been standard on every x86_64 CPU since Nehalem (2008),
// so we target it unconditionally — no runtime CPUID dispatch, no
// software table fallback. Linux 6.1+ on x86_64 guarantees it.
//
// Used for per-record corruption detection in the WAL (v8 format).
// CRC32C was chosen over CRC32/IEEE for better Hamming distance on
// short messages (58..131072 bits) and over xxHash/MurmurHash because
// non-cryptographic hashes optimize for distribution uniformity, not
// for mathematically-guaranteed bit-flip detection.
__attribute__((hot))
uint32
wal_crc32c(uint32 crc, const void *buf, size_t n)
{
    const uint8_t *p = (const uint8_t *)buf;

    // Process 8 bytes per iteration; _mm_crc32_u64 returns uint64
    // but only the low 32 bits are meaningful for CRC32C state.
    // __builtin_memcpy handles unaligned loads without UB; modern
    // x86_64 has no penalty for unaligned 8-byte reads.
    while (n >= 8) {
        uint64_t v;
        __builtin_memcpy(&v, p, 8);
        crc = (uint32)_mm_crc32_u64((uint64_t)crc, v);
        p += 8;
        n -= 8;
    }

    // Tail: up to 7 bytes.
    while (n--) {
        crc = _mm_crc32_u8(crc, *p++);
    }

    return crc;
}
