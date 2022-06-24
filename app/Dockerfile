## Build Stage
FROM golang:1.17-alpine3.15 AS build 

RUN apk update \
    && apk add build-base

WORKDIR /app

COPY go.mod go.sum main.go scripts/setup.sh ./
COPY pkg pkg
COPY cmd cmd
#COPY vendor vendor
RUN mkdir -p configs
RUN chmod +x setup.sh \
    && ./setup.sh

## Deploy Stage
FROM alpine:3.15.0
#FROM gcr.io/distroless/static

LABEL maintainer="gainchang620@gmail.com"

ENV APP_USER edp
ENV APP_UID 666

WORKDIR /app

RUN adduser \
    --disabled-password \
    --no-create-home \
    --uid ${APP_UID} \
    ${APP_USER}; \
    chown -R ${APP_USER} /app

COPY --from=build /app/bin/event-data-pipeline ./
COPY --from=build /app/configs ./configs

USER ${APP_USER}

ENTRYPOINT [ "./event-data-pipeline" ]