FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc6-dev make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN make clean && make \
    CFLAGS="-O2 -flto -fomit-frame-pointer -DNDEBUG" \
    LDFLAGS="-flto -s"

FROM debian:bookworm-slim
COPY --from=builder /src/beanstalkd /usr/bin/beanstalkd
EXPOSE 11300
ENTRYPOINT ["/usr/bin/beanstalkd"]
