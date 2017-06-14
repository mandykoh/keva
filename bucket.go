package keva

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// ErrValueNotFound indicates that a corresponding value was not found for a key.
var ErrValueNotFound = errors.New("value not found")

type bucket struct {
	id        string
	path      bucketPath
	needsSave bool
	objects   map[string][]byte
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

	absFilePath := filepath.Join(rootPath, b.path.PathString())

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
	b.needsSave = true
	return nil
}

func (b *bucket) Remove(key string) {
	delete(b.objects, key)
	b.needsSave = true
}

func (b *bucket) Save(rootPath string) error {
	if !b.needsSave {
		return nil
	}

	absFilePath := filepath.Join(rootPath, b.path.PathString())

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

	err = os.Rename(absFilePath+".swp", absFilePath)
	if err != nil {
		return err
	}

	b.needsSave = false
	return nil
}

func (b *bucket) Split(s *Store) error {
	absFilePath := filepath.Join(s.rootPath, b.path.PathString())

	os.Rename(absFilePath, absFilePath+".swp")

	err := os.Mkdir(absFilePath, os.FileMode(0700))
	if err != nil {
		os.Rename(absFilePath+".swp", absFilePath)
		return err
	}

	for key, encodedValue := range b.objects {
		bucket, err := s.bucketForKey(key)
		if err != nil {
			os.RemoveAll(absFilePath)
			os.Rename(absFilePath+".swp", absFilePath)
			return err
		}

		bucket.objects[key] = encodedValue
		bucket.needsSave = true
	}

	os.Remove(absFilePath + ".swp")
	b.needsSave = false
	return nil
}

func (b *bucket) availablePath(rootPath string) (bucketPath, error) {
	var filePath = rootPath
	var path = bucketPath(b.id)
	var step string

	for step, path = path.Step(); step != ""; step, path = path.Step() {
		filePath = filepath.Join(filePath, step)

		fileInfo, err := os.Stat(filePath)
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

	return bucketPath(b.id[0 : len(b.id)-len(path)]), nil
}

func (b *bucket) initPath(rootPath string) error {
	var err error
	b.path, err = b.availablePath(rootPath)
	return err
}
