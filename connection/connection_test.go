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
	var data =make([]byte, 1024*1024)
	_, err := r.ReadObject(BucketData, oid, data, 0)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(data))
}

func TestRedis(t *testing.T) {
	fmt.Println("redis unit test...")
	t.Run("write", testRedisPutMetadata)
	t.Run("read", testRedisGetMetadata)
	t.Run("delete", testRedisDeleteMetadata)
}

func testRedisPutMetadata(t *testing.T) {
	fmt.Println("redis put...")
	data := "Hello World"
	err := re.PutMetadata(oid, data);
	if err != nil {
		t.Error(err)
	}
}

func testRedisGetMetadata(t *testing.T) {
	fmt.Println("redis get...")
	data, err := re.GetMetadata(oid)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(data)
}

func testRedisDeleteMetadata(t *testing.T) {
	fmt.Println("redis delete...")
	err := re.DeleteMetadata(oid)
	if err != nil {
		t.Error(err)
	}
}

func TestMySQL(t *testing.T) {
	fmt.Println("mysql unit test...")
	t.Run("write", testMySQLPutMetadata)
	t.Run("read", testMySQLGetMetadata)
}

func testMySQLPutMetadata(t *testing.T) {

}

func testMySQLGetMetadata(t *testing.T) {

}