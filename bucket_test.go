package keva

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestBucketGettingNonexistentValueProducesError(t *testing.T) {
	var b bucket
	var dummy string

	err := b.Get("not-a-valid-key", &dummy)
	if err == nil {
		t.Fatalf("Expected ErrValueNotFound error but got nothing")
	}
	if err != ErrValueNotFound {
		t.Fatalf("Expected ErrValueNotFound but got other error: %v", err)
	}
}

func TestBucketLoadSucceedsWhenFileDoesNotExist(t *testing.T) {
	var b bucket
	err := b.Load("non-existent-root-path", "bucket-id")
	if err != nil {
		t.Fatalf("Error while loading bucket: %v", err)
	}

	err = b.Put("some value", "abc123")
	if err != nil {
		t.Fatalf("Error while adding value to bucket: %v", err)
	}
}

func TestBucketObjectCount(t *testing.T) {
	var b = newBucket("bucket")

	if expected, count := 0, b.ObjectCount(); expected != count {
		t.Errorf("Expected %d objects but got %d", expected, count)
	}

	b.Put("a", 1)

	if expected, count := 1, b.ObjectCount(); expected != count {
		t.Errorf("Expected %d objects but got %d", expected, count)
	}

	b.Put("b", 2)

	if expected, count := 2, b.ObjectCount(); expected != count {
		t.Errorf("Expected %d objects but got %d", expected, count)
	}
}

func TestBucketPathFindsFirstNonDirectory(t *testing.T) {
	var rootPath, err = ioutil.TempDir("", "keva-bucket-test")
	if err != nil {
		t.Fatalf("Error creating temporary location for bucket: %v", err)
	}
	defer os.RemoveAll(rootPath)

	var b = newBucket("aabbc")

	result, err := b.path(rootPath)
	if err != nil {
		t.Fatalf("Error while generating bucket path: %v", err)
	}
	if expected := filepath.Join(rootPath, "aa"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
	}

	os.MkdirAll(result, os.FileMode(0700))

	result, err = b.path(rootPath)
	if err != nil {
		t.Fatalf("Error while generating bucket path: %v", err)
	}
	if expected := filepath.Join(rootPath, "aa", "bb"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
	}

	os.MkdirAll(result, os.FileMode(0700))

	result, err = b.path(rootPath)
	if err != nil {
		t.Fatalf("Error while generating bucket path: %v", err)
	}
	if expected := filepath.Join(rootPath, "aa", "bb", "c"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
	}
}

func TestBucketRemove(t *testing.T) {
	var b = newBucket("aabb")

	err := b.Put("some-key", "hello")
	if err != nil {
		t.Fatalf("Error adding item to bucket: %v", err)
	}

	b.Remove("some-key")

	var value string
	err = b.Get("some-key", &value)
	if err == nil {
		t.Fatalf("Expected value to have been removed but got '%v'", value)
	}
	if err != ErrValueNotFound {
		t.Fatalf("Expected value to have been removed but got error: %v", err)
	}
}

func TestBucketRoundTrip(t *testing.T) {
	var rootPath, err = ioutil.TempDir("", "keva-bucket-test")
	if err != nil {
		t.Fatalf("Error creating temporary location for bucket: %v", err)
	}

	defer os.RemoveAll(rootPath)

	var b1 = newBucket("aabb")
	b1.Put("keyToTheApple", testValue{Name: "apple", Colour: "red"})

	err = b1.Save(rootPath)
	if err != nil {
		t.Fatalf("Error saving bucket: %v", err)
	}

	var b2 bucket
	err = b2.Load(rootPath, b1.id)
	if err != nil {
		t.Fatalf("Error loading bucket: %v", err)
	}

	var value testValue
	err = b2.Get("keyToTheApple", &value)
	if err != nil {
		t.Fatalf("Error fetching saved value: %v", err)
	}

	if value.Name != "apple" {
		t.Errorf("Expected value 'apple' but got %s", value.Name)
	}
	if value.Colour != "red" {
		t.Errorf("Expected value 'red' but got %s", value.Colour)
	}
}

func TestBucketSplitPushesObjectsToSubdirectories(t *testing.T) {
	var rootPath, err = ioutil.TempDir("", "keva-bucket-test")
	if err != nil {
		t.Fatalf("Error creating temporary location for bucket: %v", err)
	}

	defer os.RemoveAll(rootPath)

	var s = NewStore(rootPath)

	var b bucket
	err = b.Load(rootPath, s.bucketIdForKey("aabb"))
	if err != nil {
		t.Fatalf("Error loading bucket: %v", err)
	}

	b.Put("aabb", "value1")
	b.Put("aacc", "value2")
	b.Save(rootPath)

	err = b.Split(s)
	if err != nil {
		t.Fatalf("Error splitting bucket: %v", err)
	}

	// Bucket with original ID should still contain first value

	err = b.Load(rootPath, s.bucketIdForKey("aabb"))
	if err != nil {
		t.Fatalf("Error loading bucket: %v", err)
	}
	if count := b.ObjectCount(); count != 1 {
		t.Errorf("Expected bucket to contain 1 object but got %d", count)
	}

	var value string

	err = b.Get("aabb", &value)
	if err != nil {
		t.Errorf("Error retrieving value from bucket: %v", err)
	}
	if value != "value1" {
		t.Errorf("Retrieved value '%s' but expected 'value1'", value)
	}

	// Second value should no longer be in this bucket

	err = b.Get("aacc", &value)
	if err == nil {
		t.Errorf("Expected error but got value '%v'", value)
	}

	// Second value should have been split into another bucket

	err = b.Load(rootPath, s.bucketIdForKey("aacc"))
	if err != nil {
		t.Fatalf("Error loading bucket: %v", err)
	}
	if count := b.ObjectCount(); count != 1 {
		t.Errorf("Expected bucket to contain 1 object but got %d", count)
	}

	err = b.Get("aacc", &value)
	if err != nil {
		t.Errorf("Error retrieving value from bucket: %v", err)
	}
	if value != "value2" {
		t.Errorf("Retrieved value '%s' but expected 'value2'", value)
	}
}
