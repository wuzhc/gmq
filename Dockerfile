FROM golang:latest
MAINTAINER "wuzhc2016@163.com"
WORKDIR $GOPATH/src/github.com/wuzhc
RUN apt-get update \
	&& apt-get install vim -y
RUN git clone https://github.com/wuzhc/gmq.git $GOPATH/src/github.com/wuzhc/gmq \
	&& git clone https://github.com/wuzhc/gmq-web.git $GOPATH/src/github.com/wuzhc/gmq-web \
	&& git clone https://github.com/wuzhc/gmq-client-go.git $GOPATH/src/github.com/wuzhc/gmq-client
RUN cd gmq \ 
	&& git checkout gmq-dev-v3  \ 
	&& make glide  \ 
	&& mkdir -p /root/.glide -v \
	&& glide mirror set https://golang.org/x/sys/unix https://github.com/golang/sys \
	&& make vendor \ 
	&& make install
