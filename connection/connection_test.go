package connection

import (
	"fmt"
	"os"
	"testing"
)

var sql = NewMySQL("root", "root", "127.0.0.1:3306", "ceph", "utf8mb4")
var re = NewRedis("tcp", "127.0.0.1:6379", "")
var r, _ = NewRados()

func TestMain(m *testing.M) {
	_ = sql.Init()
	_ = re.Init()
	_ = r.InitDefault()
	exitCode := m.Run()
	os.Exit(exitCode)
}

var oid = "testid"

func TestRados(t *testing.T) {
	fmt.Println("rados unit test...")
	t.Run("write", testRadosWriteObject)
	t.Run("read", testRadosReadObject)
	t.Run("write xattr", testRadosSetXattr)
	t.Run("read xattr", testRadosGetXattr)
	t.Run("write omap", testRadosSetOmap)
	t.Run("read omap", testRadosGetOmap)
}

func testRadosWriteObject(t *testing.T) {
	fmt.Println("rados write...")
	data := []byte("Hello World")
	err := r.WriteObject(BucketData, oid, data, 0)
	if err != nil {
		t.Error(err)
	}
}

func testRadosReadObject(t *testing.T) {
	fmt.Println("rados read...")
	var data = make([]byte, 1024*1024)
	_, err := r.ReadObject(BucketData, oid, data, 0)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(data))
}

func testRadosSetXattr(t *testing.T) {
	fmt.Println("rados set xattr...")
	err := r.SetXattr(BucketData, oid, "test", []byte("Hello World"))
	if err != nil {
		t.Error(err)
	}
}

func testRadosGetXattr(t *testing.T) {
	fmt.Println("rados get xattr...")
	var data = make([]byte, 1024*1024)
	n, err := r.GetXattr(BucketData, oid, "test", data)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(data[:n]))
}

func testRadosSetOmap(t *testing.T) {
	fmt.Println("rados set omap...")
	m := make(map[string][]byte)
	m["first"] = []byte("Hello World")
	err := r.SetOmap(BucketData, oid, m)
	if err != nil {
		t.Error(err)
	}
}

func testRadosGetOmap(t *testing.T) {
	fmt.Println("rados get omap...")
	m := make(map[string][]byte)
	m, err := r.GetOmap(BucketData, oid)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(m["first"])
}

func TestRedis(t *testing.T) {
	fmt.Println("redis unit test...")
	t.Run("write", testRedisPutMetadata)
	t.Run("read", testRedisGetMetadata)
	t.Run("delete", testRedisDeleteMetadata)
}

func TestRedisExistsKey(t *testing.T) {
	exists, err := re.ExistsKey("bucket1.object0s-metadata-s")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(exists)
}

func testRedisPutMetadata(t *testing.T) {
	fmt.Println("redis put...")
	data := "Hello World"
	err := re.SetDataByString(oid, data)
	if err != nil {
		t.Error(err)
	}
}

func testRedisGetMetadata(t *testing.T) {
	fmt.Println("redis get...")
	data, err := re.GetDataByString(oid)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(data)
}

func testRedisDeleteMetadata(t *testing.T) {
	fmt.Println("redis delete...")
	err := re.Delete(oid)
	if err != nil {
		t.Error(err)
	}
}

func TestMySQL(t *testing.T) {
	fmt.Println("mysql unit test...")
	t.Run("write", testMySQLPutMetadata)
	t.Run("read", testMySQLGetMetadata)
	t.Run("delete", testMySQLDeleteMetadata)
}

func testMySQLPutMetadata(t *testing.T) {
	fmt.Println("mysql put...")
	err := sql.PutMetadata(oid, "Hello World")
	if err != nil {
		t.Error(err)
	}
}

func testMySQLGetMetadata(t *testing.T) {
	fmt.Println("mysql get...")
	objectMetadata := sql.GetMetadata(oid)
	fmt.Println(objectMetadata.Metadata)
}

func testMySQLDeleteMetadata(t *testing.T) {
	fmt.Println("mysql delete...")
	err := sql.DeleteObjectMetadata(oid)
	if err != nil {
		t.Error(err)
	}
}
