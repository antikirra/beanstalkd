FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc6-dev make binutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The image is a distributable artifact: never -march=native (it bakes the
# build host's ISA, e.g. AVX-512, into the binary -> SIGILL on older CPUs).
# On amd64 pin the documented project floor x86-64-v2 (SSE4.2-class,
# Nehalem 2008 -- see crc32c.c); other arches keep the compiler baseline.
# TARGETARCH is set automatically by BuildKit; empty under legacy builders,
# which also degrades safely to the compiler baseline.
#
# Hardening. The server parses bytes from the network and from binlogs
# it did not necessarily write, so the shipped binary carries a stack
# canary on every function with a buffer, checked libc calls
# (_FORTIFY_SOURCE=3), stack-clash probes, and control-flow protection
# where the ISA has it: CET on amd64, PAC/BTI on arm64. Cost, counted in
# user-space instructions per command on arm64: +6% pipelined, +8%
# serial — under 2% of the CPU a command costs, most of which is the
# kernel's (doc/experiments.md, E-0003).
ARG TARGETARCH
RUN ARCH=""; \
    [ "$TARGETARCH" = "amd64" ] && ARCH="-march=x86-64-v2 -fcf-protection=full"; \
    [ "$TARGETARCH" = "arm64" ] && ARCH="-mbranch-protection=standard"; \
    make clean && make \
    CFLAGS="-O2 -flto -fomit-frame-pointer $ARCH -fno-plt -fno-semantic-interposition -fvisibility=hidden -fipa-pta -fmerge-all-constants -fdata-sections -ffunction-sections -DNDEBUG -fstack-protector-strong -fstack-clash-protection -D_FORTIFY_SOURCE=3" \
    LDFLAGS="-flto -s -Wl,-z,now -Wl,-z,relro -Wl,-z,noexecstack -Wl,--gc-sections"

# Hardening gate: with LTO the code is generated at link time, and a
# flag that did not make it there is dropped without a word. So ask the
# binary: canary and checked calls imported, PIE, read-only relocations
# bound at load, no executable stack.
RUN nm -D beanstalkd | grep -q __stack_chk_fail \
 && nm -D beanstalkd | grep -q '_chk@' \
 && readelf -h beanstalkd | grep -q 'Type:.*DYN' \
 && readelf -lW beanstalkd | grep -q GNU_RELRO \
 && readelf -dW beanstalkd | grep -q BIND_NOW \
 && readelf -lW beanstalkd | grep GNU_STACK | grep -qv 'RWE'

# ISA-portability gate: the binary must not contain AVX (%ymm/%zmm)
# instructions. Fails the build if -march=native or -mavx* sneaks back in
# on an AVX-capable build host; trivially passes on non-x86 arches.
RUN ! objdump -d beanstalkd | grep -Eq '%[yz]mm'

FROM debian:bookworm-slim
COPY --from=builder /src/beanstalkd /usr/bin/beanstalkd
EXPOSE 11300
ENTRYPOINT ["/usr/bin/beanstalkd"]
