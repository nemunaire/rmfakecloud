package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	)
	if err != nil {
		return nil, err
	}

	s3opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.PathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3opts...)

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
			err = e
			return
		}
		docurl = req.URL
	} else {
		req, e := fs.presign.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(fs.Bucket),
			Key:    aws.String(blobid),
		}, s3.WithPresignExpires(duration))
		if e != nil {
			err = e
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
		err = e
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
	}

	reader = result.Body
	return
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
	return err
}
