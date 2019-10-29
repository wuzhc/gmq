FROM golang:latest
MAINTAINER "wuzhc2016@163.com"
RUN apt-get update \
	&& apt-get install vim -y
RUN git clone https://github.com/wuzhc/gmq.git $GOPATH/src/github.com/wuzhc/gmq \
	&& git clone https://github.com/wuzhc/gmq-web.git $GOPATH/src/github.com/wuzhc/gmq-web \
	&& git clone https://github.com/wuzhc/gmq-client-go.git $GOPATH/src/github.com/wuzhc/gmq-client
ADD glide-v0.13.3-linux-amd64.tar.gz /data 
RUN cd /data/linux-amd64 \
	&& mv glide $GOPATH/bin/ \
	&& mkdir -p /root/.glide -v \
	&& glide mirror set https://golang.org/x/sys/unix https://github.com/golang/sys 
WORKDIR $GOPATH/src/github.com/wuzhc
RUN cd gmq \ 
	&& git checkout gmq-dev-v3  \ 
	&& make vendor \ 
	&& make install
