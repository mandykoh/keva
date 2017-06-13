package keva

type bucketCacheEntry struct {
	bucket *bucket
	prev   *bucketCacheEntry
	next   *bucketCacheEntry
}

func (e *bucketCacheEntry) Init() *bucketCacheEntry {
	e.next = e
	e.prev = e
	return e
}

func (e *bucketCacheEntry) SpliceAfter(dest *bucketCacheEntry) *bucketCacheEntry {
	e.next.prev = e.prev
	e.prev.next = e.next
	e.next = dest.next
	e.prev = dest
	e.next.prev = e
	e.prev.next = e
	return e
}
