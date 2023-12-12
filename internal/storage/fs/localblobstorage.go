package fs

import (
	"bytes"
	"io"
)

// LocalBlobStorage local file system storage
type LocalBlobStorage struct {
	fs  *FileSystemStorage
	uid string
}

func (p *LocalBlobStorage) GetRootIndex() (string, int64, error) {
	return p.fs.GetRootIndex(p.uid)
}

func (p *LocalBlobStorage) WriteRootIndex(lastGen int64, hash string) (int64, error) {
	return p.fs.UpdateRootIndex(p.uid, bytes.NewBufferString(hash), lastGen)
}

// GetReader reader for a given hash
func (p *LocalBlobStorage) GetReader(hash string) (io.ReadCloser, error) {
	r, _, err := p.fs.LoadBlob(p.uid, hash)
	return r, err
}

// Write stores the reader in the hash
func (p *LocalBlobStorage) Write(hash string, r io.Reader) error {
	err := p.fs.StoreBlob(p.uid, hash, r)

	return err
}
