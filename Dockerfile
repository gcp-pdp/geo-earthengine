FROM golang:1.16-alpine3.13 as builder

RUN apk add --no-cache git
ENV GOPATH /go
RUN GO111MODULE=auto go get -u github.com/googlecloudplatform/gcsfuse

FROM osgeo/gdal:alpine-normal-3.3.0

RUN apk add --update --no-cache \
 curl \
 bash \
 py3-pip \
 ca-certificates \
 fuse

COPY --from=builder /go/bin/gcsfuse /usr/local/bin

#RUN curl -sSL https://sdk.cloud.google.com | bash

COPY ./scripts /root/scripts

RUN python3 -m pip install gdal numpy

RUN mkdir -p /usr/share/gcs/data

#ENV PATH $PATH:/root/google-cloud-sdk/bin:/root/scripts
ENV PATH $PATH:/root/scripts

WORKDIR /root/scripts