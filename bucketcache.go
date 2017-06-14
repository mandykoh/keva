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

func (c *bucketCache) Close(rootPath string) error {
	err := c.Flush(rootPath)
	if err != nil {
		return err
	}

	c.Clear()
	return nil
}

func (c *bucketCache) Evict(bucketID string, rootPath string) error {
	e := c.trieRoot.Remove(bucketPath(bucketID))
	if e != nil {
		err := e.bucket.Save(rootPath)
		if err != nil {
			return err
		}

		e.SpliceAfter(&c.freeEntries)
		c.bucketsCached--
	}

	return nil
}

func (c *bucketCache) Fetch(bucketID string, rootPath string, fetch func(string) (*bucket, error)) (*bucket, error) {
	b := c.lookup(bucketID)
	if b != nil {
		return b, nil
	}

	b, err := fetch(bucketID)
	if err != nil {
		return nil, err
	}

	err = c.encache(b, rootPath)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (c *bucketCache) Flush(rootPath string) error {
	for e := c.usedEntries.next; e != &c.usedEntries; e = e.next {
		err := e.bucket.Save(rootPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *bucketCache) SetMaxBucketsCached(n int, rootPath string) error {
	err := c.Flush(rootPath)
	if err != nil {
		return err
	}

	c.maxBucketsCached = n
	c.Clear()
	return nil
}

func (c *bucketCache) encache(b *bucket, rootPath string) error {
	var e *bucketCacheEntry

	if c.bucketsCached >= c.maxBucketsCached {
		e = c.usedEntries.prev
		c.trieRoot.Remove(e.bucket.path)
		err := e.bucket.Save(rootPath)
		if err != nil {
			return err
		}

	} else {
		c.bucketsCached++
		e = c.freeEntries.next
	}

	e.SpliceAfter(&c.usedEntries)
	e.bucket = b

	c.trieRoot.Insert(e)
	return nil
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
