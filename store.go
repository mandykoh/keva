package keva

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
)

const DefaultMaxObjectsPerBucket = 64
const DefaultMaxBucketsCached = 128

type Store struct {
	maxObjectsPerBucket int
	rootPath            string
	cache               *bucketCache
}

func (s *Store) Close() error {
	return s.cache.Close(s.rootPath)
}

func (s *Store) Destroy() error {
	s.cache.Clear()
	return os.RemoveAll(s.rootPath)
}

func (s *Store) Flush() error {
	return s.cache.Flush(s.rootPath)
}

func (s *Store) Get(key string, dest interface{}) error {
	bucket, err := s.bucketForKey(key)
	if err != nil {
		return err
	}

	return bucket.Get(key, dest)
}

func (s *Store) Put(key string, value interface{}) error {
	id := s.bucketIDForKey(key)
	bucket, err := s.bucketForID(id)
	if err != nil {
		return err
	}

	err = bucket.Put(key, value)
	if err != nil {
		return err
	}

	if bucket.ObjectCount() > s.maxObjectsPerBucket {
		err = s.cache.Evict(id, s.rootPath)
		if err != nil {
			return err
		}

		return bucket.Split(s)
	}

	return nil
}

func (s *Store) Remove(key string) error {
	bucket, err := s.bucketForKey(key)
	if err != nil {
		return err
	}

	bucket.Remove(key)
	return nil
}

func (s *Store) SetMaxBucketsCached(n int) *Store {
	s.cache.SetMaxBucketsCached(n)
	return s
}

func (s *Store) SetMaxObjectsPerBucket(n int) *Store {
	s.maxObjectsPerBucket = n
	return s
}

func (s *Store) bucketForKey(key string) (*bucket, error) {
	return s.bucketForID(s.bucketIDForKey(key))
}

func (s *Store) bucketForID(id string) (*bucket, error) {
	return s.cache.Fetch(id, s.rootPath, s.loadBucketForID)
}

func (s *Store) bucketIDForKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

func (s *Store) loadBucketForID(id string) (*bucket, error) {
	var b bucket
	err := b.Load(s.rootPath, id)
	if err != nil {
		return nil, err
	}

	return &b, nil
}

func NewStore(rootPath string) *Store {
	return &Store{
		maxObjectsPerBucket: DefaultMaxObjectsPerBucket,
		rootPath:            rootPath,
		cache:               newBucketCache(DefaultMaxBucketsCached),
	}
}
