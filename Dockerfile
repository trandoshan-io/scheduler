# build image
FROM golang:1.13.1-alpine3.10 as builder

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh

RUN go get -v github.com/nats-io/nats.go/

COPY . /app/
WORKDIR /app

RUN go build -v scheduler.go


# runtime image
FROM alpine:latest
COPY --from=builder /app/scheduler /app/

WORKDIR /app/
CMD ["./scheduler"]