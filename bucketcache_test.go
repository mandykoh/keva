package keva

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestBucketCache(t *testing.T) {

	t.Run("Clear() removes all cached values", func(t *testing.T) {
		b1 := newBucket("bucket1")
		b1.path = "ab"

		b2 := newBucket("bucket2")
		b2.path = "ab"

		c := newBucketCache(DefaultMaxBucketsCached)

		b := b1
		c.Fetch("ab", "", func(string) (*bucket, error) { return b, nil })

		c.Clear()

		b = b2
		result, err := c.Fetch("ab", "", func(string) (*bucket, error) { return b, nil })
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if result != b2 {
			t.Errorf("Expected bucket2 %v but got %v", b1, result)
		}
	})

	t.Run("Evict() removes cached value", func(t *testing.T) {
		b1 := newBucket("bucket1")
		b1.path = "ab"

		b2 := newBucket("bucket2")
		b2.path = "ab"

		c := newBucketCache(DefaultMaxBucketsCached)

		b := b1
		c.Fetch("ab", "", func(string) (*bucket, error) { return b, nil })

		c.Evict("ab", "")

		b = b2
		result, err := c.Fetch("ab", "", func(string) (*bucket, error) { return b, nil })
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if result != b2 {
			t.Errorf("Expected bucket2 %v but got %v", b1, result)
		}
	})

	t.Run("Fetch() delegates to fetcher function", func(t *testing.T) {
		b := newBucket("bucket")
		c := newBucketCache(DefaultMaxBucketsCached)

		result, err := c.Fetch("ab", "", func(string) (*bucket, error) { return b, nil })
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if result != b {
			t.Errorf("Expected bucket %v but got %v", b, result)
		}
	})

	t.Run("Fetch() only caches requested number of values", func(t *testing.T) {
		count := 0

		c := newBucketCache(2)

		fetch := func(id string) (*bucket, error) {
			count++

			newID := fmt.Sprintf("%s-%d", id, count)
			b := newBucket(newID)
			b.path = bucketPath(id)

			return b, nil
		}

		// First fetch should get a new bucket 01-1

		result, err := c.Fetch("01", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Second fetch should get a new bucket 02-2

		result, err = c.Fetch("02", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "02-2"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching the first ID again should return cached 01-1

		result, err = c.Fetch("01", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching a new ID should get a new bucket 03-3 (and evict 02-2)

		result, err = c.Fetch("03", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "03-3"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching the first ID again should still return cached 01-1

		result, err = c.Fetch("01", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Second ID should have been evicted, so fetching it again should get a
		// new bucket 02-4.

		result, err = c.Fetch("02", "", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "02-4"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}
	})

	t.Run("Fetch() flushes evicted buckets to disk", func(t *testing.T) {
		rootPath, err := ioutil.TempDir("", "keva-test")
		if err != nil {
			t.Fatalf("Could not create temporary location: %v", err)
		}

		c := newBucketCache(2)

		fetch := func(id string) (*bucket, error) {
			b := newBucket(id)
			b.path = bucketPath(id)
			return b, nil
		}

		// First fetch should get a new bucket 01

		result, err := c.Fetch("01", rootPath, fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Second fetch should get a new bucket 02

		bucketToEvict, err := c.Fetch("02", rootPath, fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "02"; bucketToEvict.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, bucketToEvict.id)
		}

		// Add something to 02 to make it dirty
		err = bucketToEvict.Put("someKey", "someValue")
		if err != nil {
			t.Fatalf("Error adding object to bucket: %v", err)
		}

		// Fetching the first ID again should return cached 01

		result, err = c.Fetch("01", rootPath, fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Bucket 02 should not have been flushed to disk yet

		evictedBucketPath := filepath.Join(rootPath, bucketToEvict.path.PathString())
		_, err = os.Stat(evictedBucketPath)
		if err != nil {
			if !os.IsNotExist(err) {
				t.Fatalf("Couldn’t stat bucket file %s: %v", evictedBucketPath, err)
			}
		}

		// Fetching a new ID should get a new bucket 03 (and evict 02)

		result, err = c.Fetch("03", rootPath, fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "03"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Bucket 02 should now exist on disk

		_, err = os.Stat(evictedBucketPath)
		if err != nil {
			if os.IsNotExist(err) {
				t.Fatalf("Expected bucket file %s to exist but it did not", evictedBucketPath)
			}
			t.Fatalf("Couldn’t stat bucket file %s: %v", evictedBucketPath, err)
		}
	})

	t.Run("Fetch() returns cached value", func(t *testing.T) {
		b1 := newBucket("bucket1")
		b1.path = bucketPath("bucket")

		b2 := newBucket("bucket2")
		b2.path = bucketPath("bucket")

		c := newBucketCache(DefaultMaxBucketsCached)

		b := b1
		c.Fetch("bucket1", "", func(string) (*bucket, error) { return b, nil })

		b = b2
		result, err := c.Fetch("bucket1", "", func(string) (*bucket, error) { return b, nil })
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if result != b1 {
			t.Errorf("Expected %v but got %v", b1.id, result.id)
		}
	})
}
