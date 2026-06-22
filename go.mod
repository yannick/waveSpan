module github.com/cwire/wavespan

go 1.26.4

require (
	connectrpc.com/connect v1.20.0
	github.com/prometheus/client_golang v1.23.2
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
	wavesdb v0.0.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/klauspost/cpuid/v2 v2.2.11 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/sys v0.44.0 // indirect
)

replace wavesdb v0.0.0 => ../wavesdb
