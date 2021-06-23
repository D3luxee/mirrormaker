# vim: set ft=make ffs=unix fenc=utf8:
# vim: set noet ts=4 sw=4 tw=72 list:
#
all: linux

validate:
	@go build ./...

linux:
	@env GOOS=linux GOARCH=amd64 go install -ldflags "-X main.buildtime=`date -u +%Y-%m-%dT%H:%M:%S%z` -X main.githash=`git rev-parse HEAD` -X main.shorthash=`git rev-parse --short HEAD` -X main.builddate=`date -u +%Y%m%d`" ./...

package: 
	scripts/build.sh
