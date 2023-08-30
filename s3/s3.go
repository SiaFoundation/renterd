package s3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"

	"github.com/Mikubill/gofakes3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

type goFakeLogger struct {
	l *zap.SugaredLogger
}

type Opts struct {
	AuthKeyPairs []string
}

type bus interface {
	AddObject(ctx context.Context, path, contractSet string, o object.Object, usedContract map[types.PublicKey]types.FileContractID) (err error)
	SearchObjects(ctx context.Context, key string, offset, limit int) (entries []api.ObjectMetadata, err error)
	UploadParams(ctx context.Context) (api.UploadParams, error)
}

type worker interface {
}

func (l *goFakeLogger) Print(level gofakes3.LogLevel, v ...interface{}) {
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
	keys, err := parsev4AuthKeys(opts.AuthKeyPairs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse auth key pairs: %w", err)
	}
	backend := &s3{
		b:      b,
		w:      w,
		logger: namedLogger,
	}
	faker := gofakes3.New(backend,
		gofakes3.WithHostBucket(false),
		gofakes3.WithLogger(&goFakeLogger{
			l: namedLogger,
		}),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
		gofakes3.WithV4Auth(keys),
	)
	return faker.Server(), nil
}

// parsev4AuthKeys parses a list of accessKey-secretKey pairs and returns a map
func parsev4AuthKeys(keyPairs []string) (map[string]string, error) {
	pairs := make(map[string]string)
	for _, pair := range keyPairs {
		keys := strings.Split(pair, ",")
		if len(keys) != 2 {
			return nil, fmt.Errorf("Invalid auth keypair %s", pair)
		}
		pairs[keys[0]] = keys[1]
	}
	return pairs, nil
}
