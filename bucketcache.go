package keva

type bucketCache struct {
	maxBucketsCached int
	usedEntries      bucketCacheEntry
	freeEntries      bucketCacheEntry
	bucketsCached    int
	buckets          []bucketCacheEntry
	trieRoot         *bucketCacheTrie
}

func (c *bucketCache) Clear() {
	c.buckets = make([]bucketCacheEntry, c.maxBucketsCached)

	c.usedEntries.Init()
	c.freeEntries.Init()

	for i := 0; i < len(c.buckets); i++ {
		e := &c.buckets[i]
		e.Init().SpliceAfter(&c.freeEntries)
	}

	c.bucketsCached = 0
	c.trieRoot = newBucketCacheTrie()
}

func (c *bucketCache) Evict(bucketID string) {
	e := c.trieRoot.Remove(bucketPath(bucketID))
	if e != nil {
		e.SpliceAfter(&c.freeEntries)
		c.bucketsCached--
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

func (c *bucketCache) SetMaxBucketsCached(n int) {
	c.maxBucketsCached = n
	c.Clear()
}

func (c *bucketCache) encache(b *bucket) {
	var e *bucketCacheEntry

	if c.bucketsCached >= c.maxBucketsCached {
		e = c.usedEntries.prev
		c.trieRoot.Remove(e.bucket.path)
	} else {
		c.bucketsCached++
		e = c.freeEntries.next
	}

	e.SpliceAfter(&c.usedEntries)
	e.bucket = b

	c.trieRoot.Insert(e)
}

func (c *bucketCache) lookup(id string) *bucket {
	e := c.trieRoot.Find(bucketPath(id))
	if e != nil {
		e.SpliceAfter(&c.usedEntries)
		return e.bucket
	}

	return nil
}

func newBucketCache(maxBucketsCached int) *bucketCache {
	c := &bucketCache{
		maxBucketsCached: maxBucketsCached,
	}
	c.Clear()

	return c
}
