package keva

import "testing"

func TestBucketCacheTrie(t *testing.T) {

	t.Run("Find() should return entry at end of a given path", func(t *testing.T) {
		var b1 = newBucket("aabbcc")
		b1.path = bucketPath("aabbcc")

		var e1 = &bucketCacheEntry{bucket: b1}
		e1.Init()

		var b2 = newBucket("aabbccdd")
		b2.path = bucketPath("aabbccdd")

		var e2 = &bucketCacheEntry{bucket: b2}
		e2.Init()

		trie := newBucketCacheTrie()
		trie.Insert(e1)
		trie.Insert(e2)

		result := trie.Find(b1.path)
		if result != e1 {
			t.Errorf("Expected entry %v but got %v", e1, result)
		}

		result = trie.Find(b2.path)
		if result != e2 {
			t.Errorf("Expected entry %v but got %v", e2, result)
		}
	})

	t.Run("Find() should return nil if unsuccessful", func(t *testing.T) {
		var b = newBucket("aabbc")
		b.path = bucketPath("aabbc")

		trie := newBucketCacheTrie()
		result := trie.Find(b.path)

		if result != nil {
			t.Errorf("Expected nil but got %v", result)
		}
	})

	t.Run("Insert() should add entry at end of recursive path", func(t *testing.T) {
		var b = newBucket("aabbc")
		b.path = bucketPath("aabbc")

		var e = &bucketCacheEntry{bucket: b}
		e.Init()

		trie := newBucketCacheTrie()
		trie.Insert(e)

		if result, expected := len(trie.children), 1; result != expected {
			t.Fatalf("Expected %d but got %d children", expected, result)
		}

		trie, ok := trie.children["aa"]
		if !ok {
			t.Fatalf("Expected child 'aa' to exist")
		}
		if result, expected := len(trie.children), 1; result != expected {
			t.Fatalf("Expected %d but got %d children", expected, result)
		}

		trie, ok = trie.children["bb"]
		if !ok {
			t.Fatalf("Expected child 'bb' to exist")
		}
		if result, expected := len(trie.children), 1; result != expected {
			t.Fatalf("Expected %d but got %d children", expected, result)
		}

		trie, ok = trie.children["c"]
		if !ok {
			t.Fatalf("Expected child 'c' to exist")
		}
		if result, expected := len(trie.children), 0; result != expected {
			t.Fatalf("Expected no children but got %d", result)
		}

		if trie.entry != e {
			t.Errorf("Expected entry %v but got %v", e, trie.entry)
		}
	})

	t.Run("Remove() should not remove nodes with children", func(t *testing.T) {
		var b1 = newBucket("aabbc")
		b1.path = bucketPath("aabbc")

		var e1 = &bucketCacheEntry{bucket: b1}
		e1.Init()

		var b2 = newBucket("aabbcc")
		b2.path = bucketPath("aabbcc")

		var e2 = &bucketCacheEntry{bucket: b2}
		e2.Init()

		trie := newBucketCacheTrie()
		trie.Insert(e1)
		trie.Insert(e2)

		trie.Remove(b1.path)

		if trie.children["aa"] == nil {
			t.Fatalf("Expected child to be preserved but was deleted")
		}
	})

	t.Run("Remove() should return and delete entry at end of a given path", func(t *testing.T) {
		var b = newBucket("aabbc")
		b.path = bucketPath("aabbc")

		var e = &bucketCacheEntry{bucket: b}
		e.Init()

		trie := newBucketCacheTrie()
		trie.Insert(e)

		result := trie.Remove(b.path)

		if result != e {
			t.Errorf("Expected entry %v but got %v", e, result)
		}

		result = trie.Find(b.path)
		if result != nil {
			t.Errorf("Expected nil but got %v", result)
		}

		if trie.children["aa"] != nil {
			t.Fatalf("Expected child to be deleted but was present")
		}
	})
}
