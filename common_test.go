package keva

type testValue struct {
	Name   string `json:"name"`
	Colour string `json:"colour"`
}

func newBucket(id string) *bucket {
	return &bucket{id: id, objects: make(map[string][]byte)}
}
