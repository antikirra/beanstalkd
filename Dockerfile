FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc6-dev make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The image is a distributable artifact: never -march=native (it bakes the
# build host's ISA, e.g. AVX-512, into the binary -> SIGILL on older CPUs).
# On amd64 pin the documented project floor x86-64-v2 (SSE4.2-class,
# Nehalem 2008 -- see crc32c.c); other arches keep the compiler baseline.
# TARGETARCH is set automatically by BuildKit; empty under legacy builders,
# which also degrades safely to the compiler baseline.
ARG TARGETARCH
RUN MARCH=""; [ "$TARGETARCH" = "amd64" ] && MARCH="-march=x86-64-v2"; \
    make clean && make \
    CFLAGS="-O2 -flto -fomit-frame-pointer $MARCH -fno-plt -fno-semantic-interposition -fvisibility=hidden -fipa-pta -fmerge-all-constants -fdata-sections -ffunction-sections -DNDEBUG" \
    LDFLAGS="-flto -s -Wl,-z,now -Wl,--gc-sections"

# ISA-portability gate: the binary must not contain AVX (%ymm/%zmm)
# instructions. Fails the build if -march=native or -mavx* sneaks back in
# on an AVX-capable build host; trivially passes on non-x86 arches.
RUN ! objdump -d beanstalkd | grep -Eq '%[yz]mm'

FROM debian:bookworm-slim
COPY --from=builder /src/beanstalkd /usr/bin/beanstalkd
EXPOSE 11300
ENTRYPOINT ["/usr/bin/beanstalkd"]
