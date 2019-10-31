EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

.PHONY: all
all: vendor clean build install

APPS = gnode gregister
.PHONY: $(APPS)
$(APPS): %:build/%

.PHONY: build
build: 
	go build -o ./build/gnode ./cmd/gnode
	go build -o ./build/gregister ./cmd/gregister
build/gnode:
	go build -o ./build/gnode ./cmd/gnode
build/gregister:	
	go build -o ./build/gregister ./cmd/gregister

.PHONY: vendor
vendor: glide.lock glide.yaml
	glide install -v

.PHONY: clean
clean:
	rm -rf ./build

install: $(APPS)
	install ./build/gnode ${GOPATH}/bin/gnode${EXT}
	install ./build/gregister ${GOPATH}/bin/gregister${EXT}

