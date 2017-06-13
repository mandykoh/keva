package keva

import (
	"path/filepath"
	"testing"
)

func TestBucketPath(t *testing.T) {

	t.Run("PathString() returns a filesystem path", func(t *testing.T) {
		var p = bucketPath("aabbc")

		if result, expected := p.PathString(), filepath.Join("aa", "bb", "c"); result != expected {
			t.Errorf("Expected '%s' but got '%s'", expected, result)
		}

		p = bucketPath("aabb")

		if result, expected := p.PathString(), filepath.Join("aa", "bb"); result != expected {
			t.Errorf("Expected '%s' but got '%s'", expected, result)
		}
	})

	t.Run("Step() returns the next step and the remainder", func(t *testing.T) {
		var p = bucketPath("aabbc")

		step, p := p.Step()

		if result, expected := step, "aa"; result != expected {
			t.Errorf("Expected step '%s' but got '%s'", expected, result)
		}
		if result, expected := p, bucketPath("bbc"); result != expected {
			t.Errorf("Expected remainder '%s' but got '%s'", expected, result)
		}

		step, p = p.Step()

		if result, expected := step, "bb"; result != expected {
			t.Errorf("Expected step '%s' but got '%s'", expected, result)
		}
		if result, expected := p, bucketPath("c"); result != expected {
			t.Errorf("Expected remainder '%s' but got '%s'", expected, result)
		}

		step, p = p.Step()

		if result, expected := step, "c"; result != expected {
			t.Errorf("Expected step '%s' but got '%s'", expected, result)
		}
		if result, expected := p, bucketPath(""); result != expected {
			t.Errorf("Expected empty remainder but got '%s'", result)
		}

		step, p = p.Step()

		if result, expected := step, ""; result != expected {
			t.Errorf("Expected empty step but got '%s'", result)
		}
		if result, expected := p, bucketPath(""); result != expected {
			t.Errorf("Expected empty remainder but got '%s'", result)
		}
	})
}
