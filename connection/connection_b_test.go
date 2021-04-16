package connection

import (
	"octopus/util"
	"testing"
)

func BenchmarkRedis_SaveMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = re.PutMetadata(id, "metadata")
	}
}

func BenchmarkRedis_GetMetadata100(b *testing.B) {

}

func BenchmarkMySQL_SaveMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = sql.PutMetadata(id, "metadata")
	}
}

func BenchmarkMySQL_GetMetadata100(b *testing.B) {

}

func BenchmarkCeph_WriteObject100(b *testing.B) {
	metadata := []byte("metadata")
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = r.WriteObject(BucketData, id, metadata, 0)
	}
}

