EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

.PHONY: all
all: clean build install

APPS = gnode
.PHONY: $(APPS)
$(APPS): %:build/%

.PHONY: build
build: 
	go build -o ./build/gnode ./cmd/gnode
	go build -o ./build/gctl ./cmd/gctl

.PHONY: clean
clean:
	rm -rf ./build

go-systemd:
	git clone https://github.com/coreos/go-systemd.git ${GOPATH}/src/github.com/coreos/go-systemd

install: $(APPS)
	install ./build/gnode ${GOPATH}/bin/gnode${EXT}
	install ./build/gctl ${GOPATH}/bin/gctl${EXT}
