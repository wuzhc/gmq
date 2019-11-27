EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

.PHONY: all
all: vendor clean build install

APPS = gnode
.PHONY: $(APPS)
$(APPS): %:build/%

.PHONY: build
build: 
	go build -o ./build/gnode ./cmd/gnode

.PHONY: clean
clean:
	rm -rf ./build

go-systemd:
	git clone https://github.com/coreos/go-systemd.git ${GOPATH}/src/github.com/coreos/go-systemd

install: $(APPS)
	install ./build/gnode ${GOPATH}/bin/gnode${EXT}