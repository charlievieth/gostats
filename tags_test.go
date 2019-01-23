package stats

import (
	"testing"
)

func TestSerializeTags(t *testing.T) {
	tags := map[string]string{"zzz": "hello", "q": "r"}
	serialized := serializeTags(tags)
	if serialized != ".__q=r.__zzz=hello" {
		t.Errorf("Serialized output (%s) didn't match expected output", serialized)
	}
}

func TestSerializeWithPerInstanceFlag(t *testing.T) {
	tags := map[string]string{"foo": "bar", "_f": "i"}
	serialized := serializeTags(tags)
	if serialized != ".___f=i.__foo=bar" {
		t.Errorf("Serialized output (%s) didn't match expected output", serialized)
	}
}

func BenchmarkSerializeTags(b *testing.B) {
	const name = "prefix"
	tags := map[string]string{
		"tag1": "val1",
		"tag2": "val2",
		"tag3": "val3",
		"tag4": "val4",
		"tag5": "val5",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = name + serializeTags(tags)
	}
}

func BenchmarkSerializeTags_One(b *testing.B) {
	const name = "prefix"
	tags := map[string]string{
		"tag1": "val1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = name + serializeTags(tags)
	}
}

func BenchmarkSerializeTags_Two(b *testing.B) {
	const name = "prefix"
	tags := map[string]string{
		"tag1": "val1",
		"tag2": "val2",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = name + serializeTags(tags)
	}
}
