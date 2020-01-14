module github.com/wuzhc/gmq

go 1.12

replace github.com/coreos/go-systemd => /data/wwwroot/go/src/github.com/coreos/go-systemd

require (
	github.com/coreos/etcd v3.3.17+incompatible
	github.com/coreos/go-systemd v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/etcd-io/etcd v3.3.17+incompatible
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1 // indirect
	github.com/kardianos/service v1.0.0
	github.com/wuzhc/bbolt v1.3.3
	github.com/yudai/gotty v1.0.1 // indirect
	go.uber.org/zap v1.13.0 // indirect
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	google.golang.org/genproto v0.0.0-20190819201941-24fa4b261c55
	google.golang.org/grpc v1.25.1
	gopkg.in/ini.v1 v1.51.0
)
