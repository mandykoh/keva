package keva

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
)

const DefaultMaxObjectsPerBucket = 64

type Store struct {
	maxObjectsPerBucket int
	rootPath            string
}

func (s *Store) Destroy() error {
	return os.RemoveAll(s.rootPath)
}

func (s *Store) Get(key string, dest interface{}) error {
	bucket, err := s.bucketForKey(key)
	if err != nil {
		return err
	}

	return bucket.Get(key, dest)
}

func (s *Store) Put(key string, value interface{}) error {
	bucket, err := s.bucketForKey(key)
	if err != nil {
		return err
	}

	err = bucket.Put(key, value)
	if err != nil {
		return err
	}

	if bucket.ObjectCount() > s.maxObjectsPerBucket {
		return bucket.Split(s)
	}

	return bucket.Save()
}

func (s *Store) Remove(key string) error {
	bucket, err := s.bucketForKey(key)
	if err != nil {
		return err
	}

	bucket.Remove(key)

	return bucket.Save()
}

func (s *Store) SetMaxObjectsPerBucket(n int) *Store {
	s.maxObjectsPerBucket = n
	return s
}

func (s *Store) bucketForKey(key string) (*bucket, error) {
	var b bucket
	err := b.Load(s.rootPath, s.bucketIdForKey(key))
	if err != nil {
		return nil, err
	}

	return &b, nil
}

func (s *Store) bucketIdForKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

func NewStore(rootPath string) *Store {
	return &Store{
		maxObjectsPerBucket: DefaultMaxObjectsPerBucket,
		rootPath:            rootPath,
	}
}
