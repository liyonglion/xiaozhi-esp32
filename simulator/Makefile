BINARY := xiaozhi-simulator

.PHONY: build test clean

build:
	GOPROXY=https://goproxy.cn,direct go build -o $(BINARY) .

test:
	GOPROXY=https://goproxy.cn,direct go test ./...

clean:
	rm -f $(BINARY)
