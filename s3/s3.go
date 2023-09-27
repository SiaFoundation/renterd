package s3

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/gofakes3"
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
	CreateBucket(ctx context.Context, name string, policy api.BucketPolicy) error
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) (buckets []api.Bucket, err error)

	AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, ETag string, usedContract map[types.PublicKey]types.FileContractID) (err error)
	CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath string) (om api.ObjectMetadata, err error)
	DeleteObject(ctx context.Context, bucket, path string, batch bool) (err error)
	Object(ctx context.Context, path string, opts ...api.ObjectsOption) (res api.ObjectsResponse, err error)
	ListObjects(ctx context.Context, bucket, prefix, marker string, limit int) (resp api.ObjectsListResponse, err error)

	AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) (err error)
	CompleteMultipartUpload(ctx context.Context, bucket, path string, uploadID string, parts []api.MultipartCompletedPart) (_ api.MultipartCompleteResponse, err error)
	CreateMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey) (api.MultipartCreateResponse, error)
	MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error)
	MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error)

	UploadParams(ctx context.Context) (api.UploadParams, error)
}

type worker interface {
	UploadObject(ctx context.Context, r io.Reader, path string, opts ...api.UploadOption) (err error)
	GetObject(ctx context.Context, path, bucket string, opts ...api.DownloadObjectOption) (api.GetObjectResponse, error)
	UploadMultipartUploadPart(ctx context.Context, r io.Reader, path, uploadID string, partNumber int, opts ...api.UploadOption) (ETag string, err error)
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
	faker, err := gofakes3.New(
		newAuthenticatedBackend(backend, opts.AuthKeyPairs),
		gofakes3.WithHostBucket(false),
		gofakes3.WithLogger(&gofakes3Logger{
			l: namedLogger,
		}),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 server: %w", err)
	}
	return faker.Server(), err
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
