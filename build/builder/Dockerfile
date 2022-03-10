FROM golang:alpine

# set mirror repository for the package management
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/' /etc/apk/repositories
RUN apk add make docker-cli git build-base bash

ENV GO111MODULE=on GOPROXY=https://goproxy.io

