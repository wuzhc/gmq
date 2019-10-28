export 
EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

.PHONY: all
all: vendor clean build run

APPS = gnode gregister
.PHONY: $(APPS)
$(APPS): %:build/%

.PHONY: build
build: 
	go build -o build/gnode ./cmd/gnode
	go build -o build/gregister ./cmd/gregister
build/gnode:
	go build -o build/gnode ./cmd/gnode
build/gregister:	
	go build -o build/gregister ./cmd/gregister

.PHONY: vendor
vendor: glide.lock glide.yaml
	glide install

.PHONY: clean
clean:
	rm -rf build

install: $(APPS)
	install build/gnode ${GOPATH}/bin/gnode${EXT}
	install build/gregister ${GOPATH}/bin/gregister${EXT}

.PHONY: glide
glide: 
	@hash glide 2>/dev/null || { \
		echo "安装依赖工具glide" && \
		curl https://glide.sh/get | sh; \
	}
