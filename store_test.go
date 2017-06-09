package keva

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestStoreDestroyRemovesDiskLocation(t *testing.T) {
	s := newTempStoreWithPrefix("keva-test", t)
	s.Destroy()

	_, err := os.Stat(s.rootPath)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("Expected store location '%v' to be deleted, but wasn’t", s.rootPath)
		}
	} else if err == nil {
		t.Fatalf("Expected an error when reading '%v' but got nothing", s.rootPath)
	}
}

func TestStoreInitialisesDiskLocation(t *testing.T) {
	s := newTempStoreWithPrefix("keva-test", t)
	defer s.Destroy()

	_, err := os.Stat(s.rootPath)
	if os.IsNotExist(err) {
		t.Fatalf("Expected store location '%v' to be created, but wasn’t", s.rootPath)
	} else if err != nil {
		t.Fatalf("Error inspecting disk location for store: %v", err)
	}
}

func TestStoreRoundTripping(t *testing.T) {
	s := newTempStoreWithPrefix("keva-test", t)
	defer s.Destroy()

	value := testValue{Name: "apple", Colour: "red"}

	err := s.Put("abc123", value)
	if err != nil {
		t.Fatalf("Error when storing value: %v", err)
	}

	var result testValue

	err = s.Get("abc123", &result)
	if err != nil {
		t.Fatalf("Error when retrieving value: %v", err)
	}

	if result != value {
		t.Errorf("Expected %v but got %v", value, result)
	}
}

func newTempStoreWithPrefix(prefix string, t *testing.T) *Store {
	rootPath, err := ioutil.TempDir("", prefix)
	if err != nil {
		t.Fatalf("Could not create temporary location for store: %v", err)
	}

	return NewStore(rootPath)
}
