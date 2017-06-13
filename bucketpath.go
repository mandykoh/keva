package keva

import (
	"bytes"
	"os"
)

const bucketPathSegmentLength = 2

type bucketPath string

func (p bucketPath) PathString() string {
	var result bytes.Buffer

	for i, end := 0, len(p); i < end; i += bucketPathSegmentLength {
		j := i + bucketPathSegmentLength
		if j >= end {
			result.WriteString(string(p[i:end]))
			break
		}

		result.WriteString(string(p[i:j]))
		result.WriteRune(os.PathSeparator)
	}

	return result.String()
}

func (p bucketPath) Step() (step string, remainder bucketPath) {
	if len(p) < bucketPathSegmentLength {
		return string(p), ""
	}

	return string(p[:2]), p[2:]
}
