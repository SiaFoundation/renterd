package s3

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"

	"go.sia.tech/gofakes3"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/object"
	"go.uber.org/zap"
)

type gofakes3Logger struct {
	l *zap.SugaredLogger
}

type Opts struct {
	AuthDisabled      bool
	HostBucketEnabled bool
	HostBucketBases   []string
}

type Bus interface {
	Bucket(ctx context.Context, bucketName string) (api.Bucket, error)
	CreateBucket(ctx context.Context, bucketName string, opts api.CreateBucketOptions) error
	DeleteBucket(ctx context.Context, bucketName string) error
	ListBuckets(ctx context.Context) (buckets []api.Bucket, err error)

	AddObject(ctx context.Context, bucket, key string, o object.Object, opts api.AddObjectOptions) (err error)
	CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey string, opts api.CopyObjectOptions) (om api.ObjectMetadata, err error)
	DeleteObject(ctx context.Context, bucket, key string) (err error)
	Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error)

	AbortMultipartUpload(ctx context.Context, bucket, key string, uploadID string) (err error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error)
	CreateMultipartUpload(ctx context.Context, bucket, key string, opts api.CreateMultipartOptions) (api.MultipartCreateResponse, error)
	MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error)
	MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error)

	S3Settings(ctx context.Context) (as api.S3Settings, err error)
	UploadParams(ctx context.Context) (api.UploadParams, error)
}

type Worker interface {
	GetObject(ctx context.Context, bucket, key string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error)
	HeadObject(ctx context.Context, bucket, key string, opts api.HeadObjectOptions) (*api.HeadObjectResponse, error)
	UploadObject(ctx context.Context, r io.Reader, bucket, key string, opts api.UploadObjectOptions) (*api.UploadObjectResponse, error)
	UploadMultipartUploadPart(ctx context.Context, r io.Reader, bucket, key, uploadID string, partNumber int, opts api.UploadMultipartUploadPartOptions) (*api.UploadMultipartUploadPartResponse, error)
}

func (l *gofakes3Logger) Print(level gofakes3.LogLevel, v ...interface{}) {
	switch level {
	case gofakes3.LogErr:
		l.l.Error(fmt.Sprint(v...))
	case gofakes3.LogWarn:
		l.l.Warn(fmt.Sprint(v...))
	case gofakes3.LogInfo:
		l.l.Info(fmt.Sprint(v...))
	case gofakes3.LogDebug:
		l.l.Debug(fmt.Sprint(v...))
	default:
		panic("unknown level")
	}
}

func New(b Bus, w Worker, logger *zap.Logger, opts Opts) (http.Handler, error) {
	logger = logger.Named("s3")
	s3Backend := &s3{
		b:      b,
		w:      w,
		logger: logger.Sugar(),
	}
	backend := gofakes3.Backend(s3Backend)
	if !opts.AuthDisabled {
		backend = newAuthenticatedBackend(s3Backend)
	}
	faker, err := gofakes3.New(
		backend,
		gofakes3.WithHostBucket(opts.HostBucketEnabled),
		gofakes3.WithHostBucketBase(opts.HostBucketBases...),
		gofakes3.WithLogger(&gofakes3Logger{l: logger.Sugar()}),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 server: %w", err)
	}
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
