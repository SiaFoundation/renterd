package s3

import (
	"context"
	"io"
	"net/http"

	"go.sia.tech/gofakes3"
	"go.sia.tech/gofakes3/signature"
)

var (
	_ gofakes3.AuthenticatedBackend = (*authenticatedBackend)(nil)
	_ gofakes3.Backend              = (*authenticatedBackend)(nil)
	_ gofakes3.MultipartBackend     = (*authenticatedBackend)(nil)
)

type (
	unauthenticatedBackend interface {
		gofakes3.Backend
		gofakes3.MultipartBackend
	}

	authenticatedBackend struct {
		backend    unauthenticatedBackend
		v4AuthPair map[string]string
	}

	permissions struct {
		ListBuckets             bool
		ListBucket              bool
		CreateBucket            bool
		BucketExists            bool
		DeleteBucket            bool
		GetObject               bool
		HeadObject              bool
		DeleteObject            bool
		PutObject               bool
		DeleteMulti             bool
		CopyObject              bool
		CreateMultipartUpload   bool
		UploadPart              bool
		ListMultipartUpload     bool
		ListParts               bool
		AbortMultipartUpload    bool
		CompleteMultipartUpload bool
	}

	contextKey int
)

var (
	permissionKey contextKey

	// rootPerms are used for requests that were successfully authenticated
	// using v4 signatures.
	rootPerms = permissions{
		ListBuckets:             true,
		ListBucket:              true,
		CreateBucket:            true,
		BucketExists:            true,
		DeleteBucket:            true,
		GetObject:               true,
		HeadObject:              true,
		DeleteObject:            true,
		PutObject:               true,
		DeleteMulti:             true,
		CopyObject:              true,
		CreateMultipartUpload:   true,
		UploadPart:              true,
		ListMultipartUpload:     true,
		ListParts:               true,
		AbortMultipartUpload:    true,
		CompleteMultipartUpload: true,
	}

	// noAccessPerms grant access to nothing.
	noAccessPerms = permissions{}
)

func permsFromCtx(ctx context.Context) permissions {
	if perms, ok := ctx.Value(permissionKey).(*permissions); ok {
		return *perms
	}
	return noAccessPerms
}

func newAuthenticatedBackend(b unauthenticatedBackend, keyPairs map[string]string) *authenticatedBackend {
	return &authenticatedBackend{
		v4AuthPair: keyPairs,
	}
}

func (b *authenticatedBackend) AuthenticationMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		perms := noAccessPerms
		if len(b.v4AuthPair) > 0 {
			if rq.Header.Get("Authorization") == "" {
				// No auth header, we use guest permissions.
				// TODO: guest permissions
			} else if result := signature.V4SignVerify(rq); result != signature.ErrNone {
				// Authentication attempted but failed.
				resp := signature.GetAPIError(result)
				w.WriteHeader(resp.HTTPStatusCode)
				w.Header().Add("content-type", "application/xml")
				_, _ = w.Write(signature.EncodeAPIErrorToResponse(resp))
				return
			} else {
				// Authenticated request, treat as root user.
				perms = rootPerms
			}
		}
		// Add permissions to context.
		ctx := context.WithValue(rq.Context(), permissionKey, &perms)
		handler.ServeHTTP(w, rq.WithContext(ctx))
	})
}

func (b *authenticatedBackend) ListBuckets(ctx context.Context) ([]gofakes3.BucketInfo, error) {
	if !permsFromCtx(ctx).ListBuckets {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListBuckets(ctx)
}

func (b *authenticatedBackend) ListBucket(ctx context.Context, bucketName string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if !permsFromCtx(ctx).ListBucket {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListBucket(ctx, bucketName, prefix, page)
}

func (b *authenticatedBackend) CreateBucket(ctx context.Context, name string) error {
	if !permsFromCtx(ctx).CreateBucket {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.CreateBucket(ctx, name)
}

func (b *authenticatedBackend) BucketExists(ctx context.Context, name string) (bool, error) {
	if !permsFromCtx(ctx).BucketExists {
		return false, gofakes3.ErrAccessDenied
	}
	return b.backend.BucketExists(ctx, name)
}

func (b *authenticatedBackend) DeleteBucket(ctx context.Context, name string) error {
	if !permsFromCtx(ctx).DeleteBucket {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteBucket(ctx, name)
}

func (b *authenticatedBackend) GetObject(ctx context.Context, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if !permsFromCtx(ctx).GetObject {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.GetObject(ctx, bucketName, objectName, rangeRequest)
}

func (b *authenticatedBackend) HeadObject(ctx context.Context, bucketName, objectName string) (*gofakes3.Object, error) {
	if !permsFromCtx(ctx).HeadObject {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.HeadObject(ctx, bucketName, objectName)
}

func (b *authenticatedBackend) DeleteObject(ctx context.Context, bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	if !permsFromCtx(ctx).DeleteObject {
		return gofakes3.ObjectDeleteResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteObject(ctx, bucketName, objectName)
}

func (b *authenticatedBackend) PutObject(ctx context.Context, bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	if !permsFromCtx(ctx).PutObject {
		return gofakes3.PutObjectResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.PutObject(ctx, bucketName, key, meta, input, size)
}

func (b *authenticatedBackend) DeleteMulti(ctx context.Context, bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	if !permsFromCtx(ctx).DeleteMulti {
		return gofakes3.MultiDeleteResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteMulti(ctx, bucketName, objects...)
}

func (b *authenticatedBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	if !permsFromCtx(ctx).CopyObject {
		return gofakes3.CopyObjectResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, meta)
}

func (b *authenticatedBackend) CreateMultipartUpload(ctx context.Context, bucket, key string, meta map[string]string) (gofakes3.UploadID, error) {
	if !permsFromCtx(ctx).CreateMultipartUpload {
		return "", gofakes3.ErrAccessDenied
	}
	return b.backend.CreateMultipartUpload(ctx, bucket, key, meta)
}

func (b *authenticatedBackend) UploadPart(ctx context.Context, bucket, object string, id gofakes3.UploadID, partNumber int, contentLength int64, input io.Reader) (resp *gofakes3.UploadPartResult, err error) {
	if !permsFromCtx(ctx).UploadPart {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.UploadPart(ctx, bucket, object, id, partNumber, contentLength, input)
}

func (b *authenticatedBackend) ListMultipartUploads(ctx context.Context, bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	if !permsFromCtx(ctx).ListMultipartUpload {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListMultipartUploads(ctx, bucket, marker, prefix, limit)
}

func (b *authenticatedBackend) ListParts(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	if !permsFromCtx(ctx).ListParts {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListParts(ctx, bucket, object, uploadID, marker, limit)
}

func (b *authenticatedBackend) AbortMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID) error {
	if !permsFromCtx(ctx).AbortMultipartUpload {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.AbortMultipartUpload(ctx, bucket, object, id)
}

func (b *authenticatedBackend) CompleteMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (resp *gofakes3.CompleteMultipartUploadResult, err error) {
	if !permsFromCtx(ctx).CompleteMultipartUpload {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.CompleteMultipartUpload(ctx, bucket, object, id, input)
}
