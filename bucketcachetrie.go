package keva

type bucketCacheTrie struct {
	entry    *bucketCacheEntry
	parent   *bucketCacheTrie
	children map[string]*bucketCacheTrie
}

func (t *bucketCacheTrie) Find(path bucketPath) *bucketCacheEntry {
	node := t

	for step, next := path.Step(); step != ""; step, next = next.Step() {
		child, ok := node.children[step]
		if !ok {
			break
		}

		node = child
	}

	return node.entry
}

func (t *bucketCacheTrie) Insert(e *bucketCacheEntry) {
	node := t

	for step, path := e.bucket.path.Step(); step != ""; step, path = path.Step() {
		child, ok := node.children[step]
		if !ok {
			child = newBucketCacheTrie()
			child.parent = node
			node.children[step] = child
		}

		node = child
	}

	node.entry = e
}

func (t *bucketCacheTrie) Remove(path bucketPath) *bucketCacheEntry {
	segments := []string{}
	node := t

	for step, next := path.Step(); step != ""; step, next = next.Step() {
		child, ok := node.children[step]
		if !ok {
			break
		}

		segments = append(segments, step)
		node = child
	}

	entry := node.entry
	node.entry = nil

	for len(node.children) == 0 && len(segments) > 0 {
		node = node.parent
		delete(node.children, segments[len(segments)-1])
		segments = segments[0 : len(segments)-1]
	}

	return entry
}

func newBucketCacheTrie() *bucketCacheTrie {
	return &bucketCacheTrie{
		children: make(map[string]*bucketCacheTrie),
	}
}
