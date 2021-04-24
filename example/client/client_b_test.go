package main

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func BenchmarkGetObjectWithCache(b *testing.B) {
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := rand.Intn(999)
		GetObject("bucket1", "object"+strconv.Itoa(n)+"s")
	}
}

func BenchmarkGetObject(b *testing.B) {
	rand.Seed(time.Now().Unix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := rand.Intn(999)
		GetObject("bucket1", "object"+strconv.Itoa(n))
	}
}
