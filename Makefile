GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
BINARY_NAME=fincas
BINARY_PATH=bin

FINCAS_VERSION=1.0.0
BUILD_TIME=`date +%F_%T`
COMMIT_ID=`git rev-parse HEAD`

all: clean build

build:
	@echo "Building..."
	@mkdir -p $(BINARY_PATH)
	$(GOBUILD) -o $(BINARY_PATH)/$(BINARY_NAME) \
		-ldflags "-X main.Version=$(FINCAS_VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.CommitID=$(COMMIT_ID)" \
		./cmd/fincas

clean:
	@echo "Cleaning..."
	@rm -rf $(BINARY_PATH)
	$(GOCLEAN)
