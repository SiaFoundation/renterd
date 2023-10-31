module go.sia.tech/renterd

go 1.20

require (
	github.com/gabriel-vasile/mimetype v1.4.2
	github.com/go-gormigrate/gormigrate/v2 v2.1.0
	github.com/google/go-cmp v0.5.9
	github.com/gotd/contrib v0.19.0
	github.com/klauspost/reedsolomon v1.11.8
	github.com/minio/minio-go/v7 v7.0.63
	github.com/montanaflynn/stats v0.7.1
	gitlab.com/NebulousLabs/encoding v0.0.0-20200604091946-456c3dc907fe
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.42.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.sia.tech/core v0.1.12-0.20231011172826-6ca0ac7b3b6b
	go.sia.tech/gofakes3 v0.0.0-20231003090232-776c144c0a19
	go.sia.tech/hostd v0.2.1-0.20231013174940-920057ff41c8
	go.sia.tech/jape v0.9.1-0.20230525021720-ecf031ecbffb
	go.sia.tech/mux v1.2.0
	go.sia.tech/siad v1.5.10-0.20230228235644-3059c0b930ca
	go.sia.tech/web/renterd v0.31.0
	go.uber.org/zap v1.25.0
	golang.org/x/crypto v0.13.0
	golang.org/x/term v0.12.0
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/mysql v1.5.1
	gorm.io/driver/sqlite v1.5.3
	gorm.io/gorm v1.25.4
	lukechampine.com/frand v1.4.2
)

require (
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da // indirect
	github.com/aws/aws-sdk-go v1.45.16 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cloudflare/cloudflare-go v0.75.0 // indirect
	github.com/dchest/threefish v0.0.0-20120919164726-3ecf4c494abf // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.2 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.4 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.5 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/mattn/go-sqlite3 v1.14.17 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/shabbyrobe/gocovmerge v0.0.0-20230507112040-c3350d9342df // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	gitlab.com/NebulousLabs/bolt v1.4.4 // indirect
	gitlab.com/NebulousLabs/demotemutex v0.0.0-20151003192217-235395f71c40 // indirect
	gitlab.com/NebulousLabs/entropy-mnemonics v0.0.0-20181018051301-7532f67e3500 // indirect
	gitlab.com/NebulousLabs/errors v0.0.0-20200929122200-06c536cf6975 // indirect
	gitlab.com/NebulousLabs/fastrand v0.0.0-20181126182046-603482d69e40 // indirect
	gitlab.com/NebulousLabs/go-upnp v0.0.0-20211002182029-11da932010b6 // indirect
	gitlab.com/NebulousLabs/log v0.0.0-20210609172545-77f6775350e2 // indirect
	gitlab.com/NebulousLabs/merkletree v0.0.0-20200118113624-07fbf710afc4 // indirect
	gitlab.com/NebulousLabs/monitor v0.0.0-20191205095550-2b0fd3e1012a // indirect
	gitlab.com/NebulousLabs/persist v0.0.0-20200605115618-007e5e23d877 // indirect
	gitlab.com/NebulousLabs/ratelimit v0.0.0-20200811080431-99b8f0768b2e // indirect
	gitlab.com/NebulousLabs/siamux v0.0.2-0.20220630142132-142a1443a259 // indirect
	gitlab.com/NebulousLabs/threadgroup v0.0.0-20200608151952-38921fbef213 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.16.0 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.sia.tech/web v0.0.0-20230817201630-c3d9328334b1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230815205213-6bfd019c3878 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230815205213-6bfd019c3878 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230815205213-6bfd019c3878 // indirect
	google.golang.org/grpc v1.57.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	nhooyr.io/websocket v1.8.7 // indirect
)
