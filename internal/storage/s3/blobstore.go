package s3

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ddvk/rmfakecloud/internal/config"
)

type S3BlobStorage struct {
	session *session.Session
	service *s3.S3
	Bucket  string
}

func NewS3BlobStorage(config *aws.Config, bucket string) (*S3BlobStorage, error) {
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	return &S3BlobStorage{
		session: sess,
		service: s3.New(sess, config),
		Bucket:  bucket,
	}, nil
}

// GetBlobURL return a url for a file to store
func (fs *S3BlobStorage) GetBlobURL(uid, blobid string, write bool) (docurl string, exp time.Time, err error) {
	exp = time.Now().Add(time.Minute * config.ReadStorageExpirationInMinutes)

	var req *request.Request
	if write {
		req, _ = fs.service.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(fs.Bucket),
			Key:    aws.String(blobid),
		})
	} else {
		req, _ = fs.service.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(fs.Bucket),
			Key:    aws.String(blobid),
		})
	}

	docurl, _, err = req.PresignRequest(time.Minute * config.ReadStorageExpirationInMinutes)
	if err != nil {
		return
	}

	return
}

// LoadBlob Opens a blob by id
func (fs *S3BlobStorage) LoadBlob(uid, blobid string) (reader io.ReadCloser, size int64, hash string, err error) {
	var resp *s3.HeadObjectOutput
	resp, err = fs.service.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fs.Bucket),
		Key:    aws.String(blobid),
	})
	if err != nil {
		return
	}

	size = *resp.ContentLength

	var result *s3.GetObjectOutput
	result, err = fs.service.GetObject(&s3.GetObjectInput{
		Bucket:       aws.String(fs.Bucket),
		Key:          aws.String(blobid),
		ChecksumMode: aws.String(s3.ChecksumModeEnabled), // Activer le mode checksum
	})
	if err != nil {
		return
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
	uploadInput := s3manager.UploadInput{
		Bucket: aws.String(fs.Bucket),
		ACL:    aws.String("public-read"),
		Key:    aws.String(id),
		Body:   stream,
	}

	if strings.HasPrefix(hash, "crc32=") {
		uploadInput.ChecksumCRC32 = aws.String(strings.TrimPrefix(hash, "crc32="))
	} else if strings.HasPrefix(hash, "crc32c=") {
		uploadInput.ChecksumCRC32C = aws.String(strings.TrimPrefix(hash, "crc32c="))
	} else if strings.HasPrefix(hash, "sha1=") {
		uploadInput.ChecksumSHA1 = aws.String(strings.TrimPrefix(hash, "sha1="))
	} else if strings.HasPrefix(hash, "sha256=") {
		uploadInput.ChecksumSHA256 = aws.String(strings.TrimPrefix(hash, "sha256="))
	} else if hash != "" {
		return fmt.Errorf("unknow hash method %q", hash)
	}

	_, err := s3manager.NewUploader(fs.session).Upload(&uploadInput)
	if err != nil {
		return err
	}

	return nil
}

// use file size as generation
func generationFromFileSize(size int64) int64 {
	//time + 1 space + 64 hash + 1 newline
	return size / 86
}
