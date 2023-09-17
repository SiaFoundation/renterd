package s3

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"

	"github.com/Mikubill/gofakes3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

type gofakes3Logger struct {
	l *zap.SugaredLogger
}

type Opts struct {
	AuthKeyPairs map[string]string
}

type bus interface {
	Bucket(ctx context.Context, name string) (api.Bucket, error)
	CreateBucket(ctx context.Context, name string) error
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) (buckets []api.Bucket, err error)

	AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, usedContract map[types.PublicKey]types.FileContractID) (err error)
	CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath string) error
	DeleteObject(ctx context.Context, bucket, path string, batch bool) (err error)
	Object(ctx context.Context, path string, opts ...api.ObjectsOption) (res api.ObjectsResponse, err error)
	SearchObjects(ctx context.Context, bucket, key string, offset, limit int) (entries []api.ObjectMetadata, err error)

	UploadParams(ctx context.Context) (api.UploadParams, error)
}

type worker interface {
	UploadObject(ctx context.Context, r io.Reader, path string, opts ...api.UploadOption) (err error)
	GetObject(ctx context.Context, path, bucket string, opts ...api.DownloadObjectOption) (api.GetObjectResponse, error)
}

func (l *gofakes3Logger) Print(level gofakes3.LogLevel, v ...interface{}) {
	switch level {
	case gofakes3.LogErr:
		l.l.Error(fmt.Sprint(v...))
	case gofakes3.LogWarn:
		l.l.Warn(fmt.Sprint(v...))
	case gofakes3.LogInfo:
		l.l.Info(fmt.Sprint(v...))
	default:
		panic("unknown level")
	}
}

func New(b bus, w worker, logger *zap.SugaredLogger, opts Opts) (http.Handler, error) {
	namedLogger := logger.Named("s3")
	backend := &s3{
		b:      b,
		w:      w,
		logger: namedLogger,
	}
	faker := gofakes3.New(backend,
		gofakes3.WithHostBucket(false),
		gofakes3.WithLogger(&gofakes3Logger{
			l: namedLogger,
		}),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
		gofakes3.WithV4Auth(opts.AuthKeyPairs),
	)
	return faker.Server(), nil
}

// Parsev4AuthKeys parses a list of accessKey-secretKey pairs and returns a map
func Parsev4AuthKeys(keyPairs []string) (map[string]string, error) {
	pairs := make(map[string]string)
	for _, pair := range keyPairs {
		keys := strings.Split(pair, ",")
		if len(keys) != 2 {
			return nil, fmt.Errorf("invalid auth keypair %s", pair)
		}
		pairs[keys[0]] = keys[1]
	}
	return pairs, nil
}
