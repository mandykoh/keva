package keva

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// ErrValueNotFound indicates that a corresponding value was not found for a key.
var ErrValueNotFound = errors.New("value not found")

const bucketPathSegmentLength = 2

type bucket struct {
	id      string
	path    string
	objects map[string][]byte
}

func (b *bucket) Get(key string, dest interface{}) error {
	encodedValue, ok := b.objects[key]
	if !ok {
		return ErrValueNotFound
	}

	return json.Unmarshal(encodedValue, &dest)
}

func (b *bucket) Load(rootPath, id string) error {
	b.id = id
	b.objects = nil

	var err error
	b.path, err = b.availablePath(rootPath)
	if err != nil {
		return err
	}

	absFilePath := filepath.Join(rootPath, b.path)

	file, err := os.Open(absFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			b.objects = make(map[string][]byte)
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(&b.objects)
}

func (b *bucket) ObjectCount() int {
	return len(b.objects)
}

func (b *bucket) Put(key string, value interface{}) error {
	encodedValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	b.objects[key] = encodedValue
	return nil
}

func (b *bucket) Remove(key string) {
	delete(b.objects, key)
}

func (b *bucket) Save(rootPath string) error {
	absFilePath := filepath.Join(rootPath, b.path)

	file, err := os.Create(absFilePath + ".swp")
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(b.objects)
	if err != nil {
		file.Close()
		return err
	}

	err = file.Sync()
	if err != nil {
		file.Close()
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return os.Rename(absFilePath+".swp", absFilePath)
}

func (b *bucket) Split(s *Store) error {
	absFilePath := filepath.Join(s.rootPath, b.path)

	os.Rename(absFilePath, absFilePath+".swp")

	err := os.Mkdir(absFilePath, os.FileMode(0700))
	if err != nil {
		os.Rename(absFilePath+".swp", absFilePath)
		return err
	}

	for key, encodedValue := range b.objects {
		bucket, err := s.loadBucketForID(s.bucketIDForKey(key))
		if err == nil {
			bucket.objects[key] = encodedValue
			err = bucket.Save(s.rootPath)
		}

		if err != nil {
			os.RemoveAll(absFilePath)
			os.Rename(absFilePath+".swp", absFilePath)
			return err
		}
	}

	os.Remove(absFilePath + ".swp")
	return nil
}

func (b *bucket) availablePath(rootPath string) (string, error) {
	var bucketPath string

	for i := 0; i < len(b.id); i += bucketPathSegmentLength {
		var endOffset = i + bucketPathSegmentLength
		if endOffset > len(b.id) {
			endOffset = len(b.id)
		}

		part := b.id[i:endOffset]
		bucketPath = filepath.Join(bucketPath, part)

		fileInfo, err := os.Stat(filepath.Join(rootPath, bucketPath))
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return "", err
		}
		if !fileInfo.IsDir() {
			break
		}
	}

	return bucketPath, nil
}

func (b *bucket) initPath(rootPath string) error {
	var err error
	b.path, err = b.availablePath(rootPath)
	return err
}
