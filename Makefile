SRV_NAME=myRedisCapture

clean:
	-rm ./bin/${SRV_NAME}

build:clean
	GOOS=linux GOARCH=amd64 go build  -o ./bin/$(SRV_NAME) -v main.go

.PHONY: init clean build