package keva

import (
	"fmt"
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
		c.Fetch("ab", func(string) (*bucket, error) { return b, nil })

		c.Clear()

		b = b2
		result, err := c.Fetch("ab", func(string) (*bucket, error) { return b, nil })
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
		c.Fetch("ab", func(string) (*bucket, error) { return b, nil })

		c.Evict("ab")

		b = b2
		result, err := c.Fetch("ab", func(string) (*bucket, error) { return b, nil })
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

		result, err := c.Fetch("ab", func(string) (*bucket, error) { return b, nil })
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

		// First fetch should get a new bucket 0101

		result, err := c.Fetch("01", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Second fetch should get a new bucket 2-2

		result, err = c.Fetch("02", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "02-2"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching the first ID again should return cached 1-1

		result, err = c.Fetch("01", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching a new ID should get a new bucket 3-3 (and evict 2-2)

		result, err = c.Fetch("03", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "03-3"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Fetching the first ID again should still return cached 1-1

		result, err = c.Fetch("01", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "01-1"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}

		// Second ID should have been evicted, so fetching it again should get a
		// new bucket 2-4.

		result, err = c.Fetch("02", fetch)
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if expected := "02-4"; result.id != expected {
			t.Errorf("Expected bucket %v but got %v", expected, result.id)
		}
	})

	t.Run("Fetch() returns cached value", func(t *testing.T) {
		b1 := newBucket("bucket1")
		b1.path = bucketPath("bucket")

		b2 := newBucket("bucket2")
		b2.path = bucketPath("bucket")

		c := newBucketCache(DefaultMaxBucketsCached)

		b := b1
		c.Fetch("bucket1", func(string) (*bucket, error) { return b, nil })

		b = b2
		result, err := c.Fetch("bucket1", func(string) (*bucket, error) { return b, nil })
		if err != nil {
			t.Errorf("Expected success but got error: %v", err)
		}
		if result != b1 {
			t.Errorf("Expected %v but got %v", b1.id, result.id)
		}
	})
}
