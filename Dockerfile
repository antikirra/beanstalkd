FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc6-dev make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN make clean && make \
    CFLAGS="-Os -flto -fomit-frame-pointer -march=native -fno-plt -fno-semantic-interposition -fvisibility=hidden -DNDEBUG" \
    LDFLAGS="-flto -s -Wl,-z,now"

FROM debian:bookworm-slim
COPY --from=builder /src/beanstalkd /usr/bin/beanstalkd
EXPOSE 11300
ENTRYPOINT ["/usr/bin/beanstalkd"]
