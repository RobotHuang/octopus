package connection

import (
	"strconv"
	"testing"
)

func BenchmarkRadosWriteObject100(b *testing.B) {
	data := []byte("metadata")
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_ = r.WriteObject(BucketData, strconv.Itoa(i), data, 0)
	}
}

func BenchmarkRadosReadObject100(b *testing.B) {
	var data = make([]byte, 1024 * 1024)
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_, _ = r.ReadObject(BucketData, strconv.Itoa(i), data, 0)
	}
}

func BenchmarkRadosSetXattr100(b *testing.B) {
	data := []byte("metadata")
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_ = r.SetXattr(BucketData, oid, strconv.Itoa(i), data)
	}
}

func BenchmarkRadosGetXattr100(b *testing.B) {
	var data = make([]byte, 1024 * 1024)
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_, _ = r.GetXattr(BucketData, oid, strconv.Itoa(i), data)
	}
}

func BenchmarkRadosSetOmap100(b *testing.B) {
	var m = make(map[string][]byte)
	for i := 0; i < 100; i++ {
		m[strconv.Itoa(i)] = []byte("metadata")
	}
	var mCached = make(map[string][]byte)
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		mCached[strconv.Itoa(i)] = m[strconv.Itoa(i)]
		_ = r.SetOmap(BucketData, oid, mCached)
		delete(mCached, strconv.Itoa(i))
	}
}

func BenchmarkRadosGetOmap100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_,_  = r.GetOmap(BucketData, oid)
	}
}

func BenchmarkRedisPutMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_ = re.PutMetadata(strconv.Itoa(i), "metadata")
	}
}

func BenchmarkRedisGetMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_, _ = re.GetMetadata(strconv.Itoa(i))
	}
}

func BenchmarkMySQLPutMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_ = sql.PutMetadata(strconv.Itoa(i), "metadata")
	}
}

func BenchmarkMySQLGetMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		_ = sql.GetMetadata(strconv.Itoa(i))
	}
}


