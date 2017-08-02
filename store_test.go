package keva

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestStore(t *testing.T) {

	newTempStoreWithPrefix := func(prefix string, t *testing.T) *Store {
		rootPath, err := ioutil.TempDir("", prefix)
		if err != nil {
			t.Fatalf("Could not create temporary location for store: %v", err)
		}

		store, err := NewStore(rootPath)
		if err != nil {
			t.Fatalf("Could not create store: %v", err)
		}

		return store
	}

	expectCacheCounts := func(s *Store, hits, misses uint64, t *testing.T) {
		info := s.Info()

		if result := info.CacheHitCount; result != hits {
			t.Errorf("Expected hit count to be %d but got %d", hits, result)
		}
		if result := info.CacheMissCount; result != misses {
			t.Errorf("Expected miss count to be %d but got %d", misses, result)
		}
	}

	t.Run("Destroy() removes disk location", func(t *testing.T) {
		s := newTempStoreWithPrefix("keva-test", t)
		s.Destroy()

		_, err := os.Stat(s.rootPath)
		if err != nil {
			if !os.IsNotExist(err) {
				t.Fatalf("Expected store location '%v' to be deleted, but wasn’t", s.rootPath)
			}
		} else if err == nil {
			t.Fatalf("Expected an error when reading '%v' but got nothing", s.rootPath)
		}
	})

	t.Run("Info() returns cache hit and miss counts", func(t *testing.T) {
		s := newTempStoreWithPrefix("keva-test", t)
		defer s.Destroy()

		expectCacheCounts(s, 0, 0, t)

		var value string

		s.Get("someKey", &value)
		expectCacheCounts(s, 0, 1, t)

		s.Put("someKey", "someValue")
		expectCacheCounts(s, 1, 1, t)

		s.Put("someKey", "someOtherValue")
		expectCacheCounts(s, 2, 1, t)

		s.Get("someOtherKey", &value)
		expectCacheCounts(s, 2, 2, t)
	})

	t.Run("Put() enforces max objects per bucket", func(t *testing.T) {
		s := newTempStoreWithPrefix("keva-test", t)
		defer s.Destroy()

		s.SetMaxBucketsCached(512)

		// Check that at the default setting, some buckets will contain
		// more than one object.

		var bucketsHaveMultipleItems = false
		for i := 0; i < 256; i++ {
			key := fmt.Sprintf("%02x", i)
			s.Put(key, i)

			b, err := s.bucketForKey(key)
			if err != nil {
				t.Fatalf("Error retrieving bucket: %v", err)
			}
			if count := b.ObjectCount(); count > 1 {
				bucketsHaveMultipleItems = true
			}
		}

		if !bucketsHaveMultipleItems {
			t.Fatalf("Pre-condition not met: no bucket had more than one item")
		}

		// Set the limit to one object per bucket and check that all buckets now
		// can only contain up to one object.

		s.SetMaxObjectsPerBucket(1)

		for i := 0; i < 256; i++ {
			key := fmt.Sprintf("%02x", i)
			s.Put(key, i)

			b, err := s.bucketForKey(key)
			if err != nil {
				t.Fatalf("Error retrieving bucket: %v", err)
			}
			if count := b.ObjectCount(); count > 1 {
				t.Fatalf("Bucket %s had %d objects when maximum was 1", b.id, count)
			}
		}
	})

	t.Run("Put() and Get() can be roundtripped", func(t *testing.T) {
		s := newTempStoreWithPrefix("keva-test", t)
		defer s.Destroy()

		value := testValue{Name: "apple", Colour: "red"}

		err := s.Put("abc123", value)
		if err != nil {
			t.Fatalf("Error when storing value: %v", err)
		}

		var result testValue

		err = s.Get("abc123", &result)
		if err != nil {
			t.Fatalf("Error when retrieving value: %v", err)
		}

		if result != value {
			t.Errorf("Expected %v but got %v", value, result)
		}
	})

	t.Run("Remove() makes existing object inaccessible", func(t *testing.T) {
		s := newTempStoreWithPrefix("keva-test", t)
		defer s.Destroy()

		err := s.Put("abc123", "hello")
		if err != nil {
			t.Fatalf("Error when storing value: %v", err)
		}

		err = s.Remove("abc123")
		if err != nil {
			t.Fatalf("Error when removing value: %v", err)
		}

		var result string

		err = s.Get("abc123", &result)
		if err == nil {
			t.Fatalf("Expected value to have been removed but got '%v'", result)
		}
		if err != nil && err != ErrValueNotFound {
			t.Fatalf("Expected value to have been removed but got error: %v", err)
		}
	})
}
