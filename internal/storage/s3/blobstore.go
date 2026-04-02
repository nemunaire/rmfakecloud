package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ddvk/rmfakecloud/internal/common"
	"github.com/ddvk/rmfakecloud/internal/config"
)

type Config struct {
	AccessKey  string
	SecretKey  string
	Region     string
	Endpoint   string
	PathStyle  bool
	BucketName string
}

type S3BlobStorage struct {
	client  *s3.Client
	presign *s3.PresignClient
	Bucket  string
}

func NewS3BlobStorage(cfg Config) (*S3BlobStorage, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 5
				o.MaxBackoff = 30 * time.Second
			})
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}

	s3opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.PathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3opts...)

	// Validate bucket accessibility at startup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.BucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("s3: bucket %q is not accessible: %w", cfg.BucketName, err)
	}

	return &S3BlobStorage{
		client:  client,
		presign: s3.NewPresignClient(client),
		Bucket:  cfg.BucketName,
	}, nil
}

// GetBlobURL returns a pre-signed URL for a blob
func (fs *S3BlobStorage) GetBlobURL(uid, blobid string, write bool) (docurl string, exp time.Time, err error) {
	duration := time.Minute * config.ReadStorageExpirationInMinutes
	exp = time.Now().Add(duration)

	ctx := context.Background()
	if write {
		req, e := fs.presign.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(fs.Bucket),
			Key:    aws.String(blobid),
		}, s3.WithPresignExpires(duration))
		if e != nil {
			err = fmt.Errorf("s3: presign put %q: %w", blobid, e)
			return
		}
		docurl = req.URL
	} else {
		req, e := fs.presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(fs.Bucket),
			Key:    aws.String(blobid),
		}, s3.WithPresignExpires(duration))
		if e != nil {
			err = fmt.Errorf("s3: presign get %q: %w", blobid, e)
			return
		}
		docurl = req.URL
	}
	return
}

// LoadBlob opens a blob by id
func (fs *S3BlobStorage) LoadBlob(uid, blobid string) (reader io.ReadCloser, size int64, hash string, err error) {
	result, e := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket:       aws.String(fs.Bucket),
		Key:          aws.String(blobid),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if e != nil {
		err = fmt.Errorf("s3: get object %q: %w", blobid, e)
		return
	}

	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	if result.ChecksumCRC32 != nil {
		hash = "crc32=" + *result.ChecksumCRC32
	} else if result.ChecksumCRC32C != nil {
		hash = "crc32c=" + *result.ChecksumCRC32C
	} else if result.ChecksumSHA1 != nil {
		hash = "sha1=" + *result.ChecksumSHA1
	} else if result.ChecksumSHA256 != nil {
		hash = "sha256=" + *result.ChecksumSHA256
	} else {
		log.Println("non hash registered for %s / %s (size=%d)", uid, blobid, size)
		// No checksum provided by S3: read the body, compute crc32c, then wrap in a new reader
		var buf bytes.Buffer
		if _, e := io.Copy(&buf, result.Body); e != nil {
			result.Body.Close()
			err = fmt.Errorf("s3: read body %q for hashing: %w", blobid, e)
			return
		}
		result.Body.Close()
		hash, err = common.CRC32CFromReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			err = fmt.Errorf("s3: compute crc32c for %q: %w", blobid, err)
			return
		}
		hash = "crc32c=" + hash
		size = int64(buf.Len())
		reader = io.NopCloser(bytes.NewReader(buf.Bytes()))
		return
	}

	reader = result.Body
	return
}

// BlobExists checks if a blob exists in S3
func (fs *S3BlobStorage) BlobExists(uid, blobid string) (bool, error) {
	_, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.Bucket),
		Key:    aws.String(blobid),
	})
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) {
			return false, nil
		}
		// Also handle the case where HeadObject returns a generic not found
		var nfe *types.NoSuchKey
		if errors.As(err, &nfe) {
			return false, nil
		}
		// For HeadObject, a 404 may come as a smithy OperationError
		// Check for HTTP 404 status
		var respErr interface{ HTTPStatusCode() int }
		if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 404 {
			return false, nil
		}
		return false, fmt.Errorf("s3: head object %q: %w", blobid, err)
	}
	return true, nil
}

// StoreBlob stores a document
func (fs *S3BlobStorage) StoreBlob(uid, id string, fileName string, hash string, stream io.Reader) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(fs.Bucket),
		Key:    aws.String(id),
		Body:   stream,
	}

	if v, ok := strings.CutPrefix(hash, "crc32="); ok {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmCrc32
		input.ChecksumCRC32 = aws.String(v)
	} else if v, ok := strings.CutPrefix(hash, "crc32c="); ok {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmCrc32c
		input.ChecksumCRC32C = aws.String(v)
	} else if v, ok := strings.CutPrefix(hash, "sha1="); ok {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha1
		input.ChecksumSHA1 = aws.String(v)
	} else if v, ok := strings.CutPrefix(hash, "sha256="); ok {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = aws.String(v)
	} else if hash != "" {
		return fmt.Errorf("unknown hash method %q", hash)
	}

	uploader := manager.NewUploader(fs.client)
	_, err := uploader.Upload(context.Background(), input)
	if err != nil {
		return fmt.Errorf("s3: upload %q: %w", id, err)
	}
	return nil
}
