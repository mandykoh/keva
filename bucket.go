package keva

import (
	"encoding/json"
	"errors"
	"os"
	"path"
)

const bucketPathSegmentLength = 2

type bucket struct {
	id      string
	objects map[string][]byte
}

func (b *bucket) Get(key string, dest interface{}) error {
	encodedValue, ok := b.objects[key]
	if !ok {
		return errors.New("not found")
	}

	return json.Unmarshal(encodedValue, &dest)
}

func (b *bucket) Load(rootPath, id string) error {
	b.id = id
	b.objects = nil

	bucketPath, err := b.path(rootPath)
	if err != nil {
		return err
	}

	file, err := os.Open(bucketPath)
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

func (b *bucket) Save(rootPath string) error {
	bucketPath, err := b.path(rootPath)
	if err != nil {
		return err
	}

	file, err := os.Create(bucketPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(b.objects)
}

func (b *bucket) Split(s *Store) error {
	bucketPath, err := b.path(s.rootPath)
	if err != nil {
		return err
	}

	os.Rename(bucketPath, bucketPath+".swp")

	err = os.Mkdir(bucketPath, os.FileMode(0700))
	if err != nil {
		os.Rename(bucketPath+".swp", bucketPath)
		return err
	}

	for key, encodedValue := range b.objects {
		bucket, err := s.bucketForKey(key)
		if err == nil {
			bucket.objects[key] = encodedValue
			err = bucket.Save(s.rootPath)
		}

		if err != nil {
			os.RemoveAll(bucketPath)
			os.Rename(bucketPath+".swp", bucketPath)
			return err
		}
	}

	os.Remove(bucketPath + ".swp")
	return nil
}

func (b *bucket) path(rootPath string) (string, error) {
	bucketPath := rootPath

	for i := 0; i < len(b.id); i += bucketPathSegmentLength {
		var endOffset = i + bucketPathSegmentLength
		if endOffset > len(b.id) {
			endOffset = len(b.id)
		}

		part := b.id[i:endOffset]
		bucketPath = path.Join(bucketPath, part)

		fileInfo, err := os.Stat(bucketPath)
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

func newBucket(id string) *bucket {
	return &bucket{id: id, objects: make(map[string][]byte)}
}
