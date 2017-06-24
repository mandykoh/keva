package keva

import (
	"crypto/sha256"
	"encoding/hex"
	"os"

	"github.com/mandykoh/symlock"
)

const DefaultMaxObjectsPerBucket = 64
const DefaultMaxBucketsCached = 128
const DefaultLockPartitions = 8

type Store struct {
	maxObjectsPerBucket int
	rootPath            string
	cache               *bucketCache
	readyToFlush        bool
	bucketLock          *symlock.SymLock
}

func (s *Store) Close() error {
	return s.cache.Close(s.rootPath)
}

func (s *Store) Destroy() error {
	s.cache.Clear()
	return os.RemoveAll(s.rootPath)
}

func (s *Store) Flush() error {
	if s.readyToFlush {
		err := s.cache.Flush(s.rootPath)
		if err != nil {
			return err
		}

		s.readyToFlush = false
	}

	return nil
}

func (s *Store) Get(key string, dest interface{}) error {
	return s.withBucketForKey(key, func(bucket *bucket) error {
		return bucket.Get(key, dest)
	})
}

func (s *Store) Info() StoreInfo {
	return StoreInfo{
		CacheHitCount:  s.cache.HitCount,
		CacheMissCount: s.cache.MissCount,
	}
}

func (s *Store) Put(key string, value interface{}) error {
	id := s.bucketIDForKey(key)

	return s.withBucketForID(id, func(bucket *bucket) error {
		err := bucket.Put(key, value)
		if err != nil {
			return err
		}

		s.readyToFlush = true

		if bucket.ObjectCount() > s.maxObjectsPerBucket {
			err = s.cache.Evict(id, s.rootPath)
			if err != nil {
				return err
			}

			return bucket.Split(s.rootPath, s.bucketForKey)
		}

		return nil
	})
}

func (s *Store) Remove(key string) error {
	return s.withBucketForKey(key, func(bucket *bucket) error {
		bucket.Remove(key)
		s.readyToFlush = true
		return nil
	})
}

func (s *Store) SetMaxBucketsCached(n int) error {
	return s.cache.SetMaxBucketsCached(n, s.rootPath)
}

func (s *Store) SetMaxObjectsPerBucket(n int) {
	s.maxObjectsPerBucket = n
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

func (s *Store) withBucketForID(id string, action func(*bucket) error) (err error) {
	s.bucketLock.WithMutex(id[0:bucketPathSegmentLength], func() {
		var bucket *bucket
		bucket, err = s.bucketForID(id)
		if err == nil {
			err = action(bucket)
		}
	})

	return
}

func (s *Store) withBucketForKey(key string, action func(*bucket) error) error {
	return s.withBucketForID(s.bucketIDForKey(key), action)
}

func NewStore(rootPath string) *Store {
	return &Store{
		maxObjectsPerBucket: DefaultMaxObjectsPerBucket,
		rootPath:            rootPath,
		cache:               newBucketCache(DefaultMaxBucketsCached),
		bucketLock:          symlock.NewWithPartitions(DefaultLockPartitions),
	}
}
