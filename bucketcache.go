package keva

import (
	"os"
	"regexp"
	"strings"
)

var pathSegmentPattern = regexp.MustCompile(`..`)

type bucketCache struct {
	maxBucketsCached int
	buckets          []*bucket
}

func (c *bucketCache) Clear() {
	c.buckets = nil
}

func (c *bucketCache) Evict(bucketID string) {
	path := c.pathForID(bucketID)

	for i := 0; i < len(c.buckets); i++ {
		if strings.HasPrefix(path, c.buckets[i].path) {
			c.buckets = append(c.buckets[:i], c.buckets[i+1:]...)
			break
		}
	}
}

func (c *bucketCache) Fetch(bucketID string, fetch func(string) (*bucket, error)) (*bucket, error) {
	b := c.lookup(bucketID)
	if b != nil {
		return b, nil
	}

	b, err := fetch(bucketID)
	if err != nil {
		return nil, err
	}

	c.encache(b)
	return b, nil
}

func (c *bucketCache) encache(b *bucket) {
	if len(c.buckets) >= c.maxBucketsCached && len(c.buckets) > 0 {
		copy(c.buckets[:len(c.buckets)-1], c.buckets[1:])
		c.buckets[len(c.buckets)-1] = b
	} else {
		c.buckets = append(c.buckets, b)
	}
}

func (c *bucketCache) lookup(id string) *bucket {
	path := c.pathForID(id)

	for i := 0; i < len(c.buckets); i++ {
		if strings.HasPrefix(path, c.buckets[i].path) {
			b := c.buckets[i]
			copy(c.buckets[i:], c.buckets[i+1:])
			c.buckets[len(c.buckets)-1] = b
			return b
		}
	}

	return nil
}

func (c *bucketCache) pathForID(id string) string {
	return pathSegmentPattern.ReplaceAllStringFunc(id, func(s string) string { return s + string(os.PathSeparator) })
}

func newBucketCache(maxBucketsCached int) *bucketCache {
	return &bucketCache{maxBucketsCached: maxBucketsCached}
}
