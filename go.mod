module github.com/cirruslabs/orchard

go 1.27

// Work around https://github.com/gin-gonic/gin/issues/4372
replace github.com/gin-gonic/gin v1.11.0 => github.com/gin-gonic/gin v1.10.0

require (
	github.com/avast/retry-go/v4 v4.7.0
	github.com/avast/retry-go/v5 v5.0.0
	github.com/cirruslabs/chacha v0.16.3
	github.com/coder/websocket v1.8.15
	github.com/deckarep/golang-set/v2 v2.9.0
	github.com/dgraph-io/badger/v3 v3.2103.5
	github.com/dustin/go-humanize v1.0.1
	github.com/gin-contrib/zap v1.1.7
	github.com/gin-gonic/gin v1.12.0
	github.com/go-openapi/runtime v0.33.1
	github.com/gofrs/flock v0.13.0
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/gosuri/uitable v0.0.4
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-version v1.9.0
	github.com/manifoldco/promptui v0.9.0
	github.com/mitchellh/go-grpc-net-conn v0.0.0-20200427190222-eb030e4876f0
	github.com/pkg/errors v0.9.1
	github.com/pterm/pterm v0.12.83
	github.com/puzpuzpuz/xsync/v4 v4.5.0
	github.com/samber/lo v1.53.0
	github.com/samber/mo v1.17.0
	github.com/sethvargo/go-password v0.4.0
	github.com/shirou/gopsutil/v4 v4.26.7
	github.com/skratchdot/open-golang v0.0.0-20200116055534-eef842397966
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.70.0
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.45.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.45.0
	go.opentelemetry.io/otel/metric v1.45.0
	go.opentelemetry.io/otel/sdk/metric v1.45.0
	go.uber.org/zap v1.28.0
	golang.org/x/crypto v0.55.0
	golang.org/x/exp v0.0.0-20250218142911-aa4b98e5adaa
	golang.org/x/net v0.58.0
	golang.org/x/sync v0.22.0
	golang.org/x/term v0.45.0
	golang.org/x/text v0.41.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
	howett.net/plist v1.0.1
)

require (
	atomicgo.dev/cursor v0.2.0 // indirect
	atomicgo.dev/keyboard v0.2.9 // indirect
	atomicgo.dev/schedule v0.1.0 // indirect
	github.com/bytedance/gopkg v0.1.4 // indirect
	github.com/bytedance/sonic v1.15.2 // indirect
	github.com/bytedance/sonic/loader v0.5.2 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cloudwego/base64x v0.1.7 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/ebitengine/purego v0.10.2 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.15 // indirect
	github.com/gin-contrib/sse v1.1.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.26.0 // indirect
	github.com/go-openapi/errors v0.22.8 // indirect
	github.com/go-openapi/jsonpointer v1.0.0 // indirect
	github.com/go-openapi/jsonreference v1.0.0 // indirect
	github.com/go-openapi/loads v0.25.1 // indirect
	github.com/go-openapi/runtime/server-middleware v0.33.1 // indirect
	github.com/go-openapi/spec v0.22.9 // indirect
	github.com/go-openapi/strfmt v0.27.0 // indirect
	github.com/go-openapi/swag/conv v0.28.0 // indirect
	github.com/go-openapi/swag/fileutils v0.28.0 // indirect
	github.com/go-openapi/swag/jsonutils v0.28.0 // indirect
	github.com/go-openapi/swag/loading v0.28.0 // indirect
	github.com/go-openapi/swag/mangling v0.28.0 // indirect
	github.com/go-openapi/swag/pools v0.28.0 // indirect
	github.com/go-openapi/swag/stringutils v0.28.0 // indirect
	github.com/go-openapi/swag/typeutils v0.28.0 // indirect
	github.com/go-openapi/swag/yamlutils v0.28.0 // indirect
	github.com/go-openapi/validate v0.26.3 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.3 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/gookit/color v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/leodido/go-urn v1.5.0 // indirect
	github.com/lithammer/fuzzysearch v1.1.8 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.61.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.mongodb.org/mongo-driver v1.17.4 // indirect
	go.mongodb.org/mongo-driver/v2 v2.8.0 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/sdk v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/arch v0.29.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
)
