.PHONY: build test lint bench run clean

BINARY_NAME := bin/relayd
PKG := ./...

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY_NAME) ./cmd/relayd

test:
	go test -v -race -count=1 $(PKG)

lint:
	go vet $(PKG)

bench:
	go test -bench=. -benchmem -run=^$$ $(PKG)

run:
	go run ./cmd/relayd

clean:
	rm -rf bin/
