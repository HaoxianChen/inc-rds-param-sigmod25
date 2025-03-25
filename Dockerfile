ARG GO_VER=1.19

FROM golang:${GO_VER}-alpine as builder

WORKDIR /rock_v3

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories \
	&& apk add \
	gcc \
	musl-dev \
    make

COPY . .
# -gcflags="all=-N -l"
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -mod vendor -o rock-v3 -ldflags="-s -w" main.go

RUN mkdir publish && cp rock-v3 publish && cp -r config/ publish

FROM alpine

RUN mkdir /rock_v3
WORKDIR /rock_v3
RUN chmod -R 777 /rock_v3/

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add tzdata delve
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone

ENV LANG zh_CN.UTF-8
ENV LC_ALL zh_CN.UTF-8
ENV LANGUAGE zh_CN.UTF-8

COPY --from=builder /rock_v3/publish /rock_v3

EXPOSE 19123 19124 19125 19126 19127 19128 19129 19130 19131 19132 19133 19134 19135 19136 19137 19138 19139 19140 19141 19142
EXPOSE 20000 20001 20002 20003 20004 20005 20006 20007 20008 20009 20010 20011 20012 20013 20014 20015 20016 20017 20018 20019
EXPOSE 21000 21001 21002 21003 21004 21005 21006 21007 21008 21009 21010 21011 21012 21013 21014 21015 21016 21017 21018 21019
EXPOSE 30000 30001 30002 30003 30004 30005 30006 30007 30008 30009 30010 30011 30012 30013 30014 30015 30016 30017 30018 30019

ENTRYPOINT /rock_v3/rock-v3 $0 $@
