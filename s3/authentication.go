package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.sia.tech/gofakes3"
	"go.sia.tech/gofakes3/signature"
	"go.sia.tech/renterd/api"
)

var (
	_ gofakes3.AuthenticatedBackend = (*authenticatedBackend)(nil)
	_ gofakes3.Backend              = (*authenticatedBackend)(nil)
	_ gofakes3.MultipartBackend     = (*authenticatedBackend)(nil)
)

type (
	authenticatedBackend struct {
		backend *s3
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

func writeResponse(w http.ResponseWriter, err signature.APIError) {
	w.WriteHeader(err.HTTPStatusCode)
	w.Header().Add("Content-Type", "application/xml")
	_, _ = w.Write(signature.EncodeAPIErrorToResponse(err))
}

func newAuthenticatedBackend(b *s3) *authenticatedBackend {
	return &authenticatedBackend{
		backend: b,
	}
}

func (b *authenticatedBackend) applyBucketPolicy(ctx context.Context, bucketName string, p *permissions) error {
	bucket, err := b.backend.b.Bucket(ctx, bucketName)
	if err != nil && strings.Contains(err.Error(), api.ErrBucketNotFound.Error()) {
		return gofakes3.BucketNotFound(bucketName)
	} else if err != nil {
		return gofakes3.ErrorMessage(gofakes3.ErrInternal, err.Error())
	}
	if bucket.Policy.PublicReadAccess {
		p.ListBucket = true
		p.BucketExists = true
		p.GetObject = true
		p.HeadObject = true
	}
	return nil
}

func (b *authenticatedBackend) permsFromCtx(ctx context.Context, bucket string) permissions {
	perms := noAccessPerms
	if p, ok := ctx.Value(permissionKey).(*permissions); ok {
		perms = *p
	}
	if bucket != "" {
		b.applyBucketPolicy(ctx, bucket, &perms)
	}
	return perms
}

func (b *authenticatedBackend) reloadV4Keys(ctx context.Context) error {
	as, err := b.backend.b.S3AuthenticationSettings(ctx)
	if err != nil {
		return err
	}
	signature.ReloadKeys(as.V4Keypairs)
	return nil
}

func (b *authenticatedBackend) AuthenticationMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		perms := noAccessPerms
		if rq.Header.Get("Authorization") == "" {
			// No auth header, we continue without permissions. Request might
			// still succeed due to bucket policy.
		} else if err := b.reloadV4Keys(rq.Context()); err != nil {
			writeResponse(w, signature.APIError{
				Code:           string(gofakes3.ErrInternal),
				Description:    fmt.Sprintf("failed to reload v4 keys: %v", err),
				HTTPStatusCode: http.StatusInternalServerError,
			})
			return
		} else if result := signature.V4SignVerify(rq); result != signature.ErrNone {
			// Authentication attempted but failed.
			writeResponse(w, signature.GetAPIError(result))
			return
		} else {
			// Authenticated request, treat as root user.
			perms = rootPerms
		}
		// Add permissions to context.
		ctx := context.WithValue(rq.Context(), permissionKey, &perms)
		handler.ServeHTTP(w, rq.WithContext(ctx))
	})
}

func (b *authenticatedBackend) ListBuckets(ctx context.Context) ([]gofakes3.BucketInfo, error) {
	if !b.permsFromCtx(ctx, "").ListBuckets {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListBuckets(ctx)
}

func (b *authenticatedBackend) ListBucket(ctx context.Context, bucketName string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if !b.permsFromCtx(ctx, bucketName).ListBucket {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListBucket(ctx, bucketName, prefix, page)
}

func (b *authenticatedBackend) CreateBucket(ctx context.Context, name string) error {
	if !b.permsFromCtx(ctx, "").CreateBucket {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.CreateBucket(ctx, name)
}

func (b *authenticatedBackend) BucketExists(ctx context.Context, name string) (bool, error) {
	if !b.permsFromCtx(ctx, name).BucketExists {
		return false, gofakes3.ErrAccessDenied
	}
	return b.backend.BucketExists(ctx, name)
}

func (b *authenticatedBackend) DeleteBucket(ctx context.Context, name string) error {
	if !b.permsFromCtx(ctx, name).DeleteBucket {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteBucket(ctx, name)
}

func (b *authenticatedBackend) GetObject(ctx context.Context, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if !b.permsFromCtx(ctx, bucketName).GetObject {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.GetObject(ctx, bucketName, objectName, rangeRequest)
}

func (b *authenticatedBackend) HeadObject(ctx context.Context, bucketName, objectName string) (*gofakes3.Object, error) {
	if !b.permsFromCtx(ctx, bucketName).HeadObject {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.HeadObject(ctx, bucketName, objectName)
}

func (b *authenticatedBackend) DeleteObject(ctx context.Context, bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	if !b.permsFromCtx(ctx, bucketName).DeleteObject {
		return gofakes3.ObjectDeleteResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteObject(ctx, bucketName, objectName)
}

func (b *authenticatedBackend) PutObject(ctx context.Context, bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	if !b.permsFromCtx(ctx, bucketName).PutObject {
		return gofakes3.PutObjectResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.PutObject(ctx, bucketName, key, meta, input, size)
}

func (b *authenticatedBackend) DeleteMulti(ctx context.Context, bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	if !b.permsFromCtx(ctx, bucketName).DeleteMulti {
		return gofakes3.MultiDeleteResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.DeleteMulti(ctx, bucketName, objects...)
}

func (b *authenticatedBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	if !b.permsFromCtx(ctx, srcBucket).CopyObject {
		return gofakes3.CopyObjectResult{}, gofakes3.ErrAccessDenied
	} else if !b.permsFromCtx(ctx, dstBucket).PutObject {
		return gofakes3.CopyObjectResult{}, gofakes3.ErrAccessDenied
	}
	return b.backend.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey, meta)
}

func (b *authenticatedBackend) CreateMultipartUpload(ctx context.Context, bucket, key string, meta map[string]string) (gofakes3.UploadID, error) {
	if !b.permsFromCtx(ctx, bucket).CreateMultipartUpload {
		return "", gofakes3.ErrAccessDenied
	}
	return b.backend.CreateMultipartUpload(ctx, bucket, key, meta)
}

func (b *authenticatedBackend) UploadPart(ctx context.Context, bucket, object string, id gofakes3.UploadID, partNumber int, contentLength int64, input io.Reader) (resp *gofakes3.UploadPartResult, err error) {
	if !b.permsFromCtx(ctx, bucket).UploadPart {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.UploadPart(ctx, bucket, object, id, partNumber, contentLength, input)
}

func (b *authenticatedBackend) ListMultipartUploads(ctx context.Context, bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	if !b.permsFromCtx(ctx, bucket).ListMultipartUpload {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListMultipartUploads(ctx, bucket, marker, prefix, limit)
}

func (b *authenticatedBackend) ListParts(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	if !b.permsFromCtx(ctx, bucket).ListParts {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.ListParts(ctx, bucket, object, uploadID, marker, limit)
}

func (b *authenticatedBackend) AbortMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID) error {
	if !b.permsFromCtx(ctx, bucket).AbortMultipartUpload {
		return gofakes3.ErrAccessDenied
	}
	return b.backend.AbortMultipartUpload(ctx, bucket, object, id)
}

func (b *authenticatedBackend) CompleteMultipartUpload(ctx context.Context, bucket, object string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (resp *gofakes3.CompleteMultipartUploadResult, err error) {
	if !b.permsFromCtx(ctx, bucket).CompleteMultipartUpload {
		return nil, gofakes3.ErrAccessDenied
	}
	return b.backend.CompleteMultipartUpload(ctx, bucket, object, id, input)
}
