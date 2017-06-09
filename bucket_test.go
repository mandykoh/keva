package keva

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestBucketGettingNonexistentValueProducesError(t *testing.T) {
	var b bucket
	var dummy string

	err := b.Get("not-a-valid-key", &dummy)
	if err == nil {
		t.Fatalf("Expected an error but got nothing")
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
	if expected := path.Join(rootPath, "aa"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
	}

	os.MkdirAll(result, os.FileMode(0700))

	result, err = b.path(rootPath)
	if err != nil {
		t.Fatalf("Error while generating bucket path: %v", err)
	}
	if expected := path.Join(rootPath, "aa", "bb"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
	}

	os.MkdirAll(result, os.FileMode(0700))

	result, err = b.path(rootPath)
	if err != nil {
		t.Fatalf("Error while generating bucket path: %v", err)
	}
	if expected := path.Join(rootPath, "aa", "bb", "c"); expected != result {
		t.Errorf("Expected path '%s' but got '%s'", expected, result)
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
