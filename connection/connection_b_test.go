package connection

import (
	"octopus/util"
	"os"
	"testing"
)

var mysql = NewMySQL("root", "root", "127.0.0.1:3306", "ceph", "utf8mb4")
var redis = NewRedis("tcp", "127.0.0.1:6379", "")
var ceph, _ = NewRados()

func testMain(m *testing.M) {
	_ = mysql.Init()
	_ = redis.Init()
	_ = ceph.InitDefault()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func BenchmarkRedis_SaveMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = redis.SaveMetadata(id, "metadata")
	}
}

func BenchmarkRedis_GetMetadata100(b *testing.B) {

}

func BenchmarkMySQL_SaveMetadata100(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = mysql.SaveMetadata(id, "metadata")
	}
}

func BenchmarkMySQL_GetMetadata100(b *testing.B) {

}

func BenchmarkCeph_WriteObject100(b *testing.B) {
	metadata := []byte("metadata")
	b.ResetTimer()
	for i := 0; i < 100; i++ {
		id := util.GenerateRandStr(8)
		_ = ceph.WriteObject(BucketData, id, metadata, 0)
	}
}

