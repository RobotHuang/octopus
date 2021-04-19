package session

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	. "octopus/connection"
	"octopus/util"
	"strings"
)

func CreateBucket(bucketName, bucketAcl string) (err error) {
	var rollback = func(rollback func()) {
		if err != nil {
			rollback()
		}
	}

	bucketId := util.GenerateRandStr(8)
	if err = RedisMgr.Redis.RPUSHData("bucket", bucketName); err != nil {
		return err
	}
	defer rollback(func() {
		_ = RedisMgr.Redis.LREMData("bucket", bucketName, 1)
	})
	if err = RedisMgr.Redis.SetDataByString(bucketName, bucketId); err != nil {
		return err
	}
	return nil
}

func ListBuckets() ([]string, error) {
	return RedisMgr.Redis.GetAllDataInList("bucket")
}

func PutObject(bucketName, objectName string, object io.ReadCloser, hash string, metadataM map[string][]string) (err error) {
	rollback := func(rollback func()) {
		if err != nil {
			rollback()
		}
	}

	// 1M
	var objectCache = make([]byte, 1024*1024)
	var data []byte
	// Read the object
	for {
		n, err := object.Read(objectCache)
		if err != nil && err != io.EOF {
			return err
		}
		data = append(data, objectCache[:n]...)
		if err == io.EOF {
			break
		}
	}

	check := md5.New()
	hashcache := bufio.NewReader(bytes.NewReader(data))
	_, err = io.Copy(check, hashcache)
	if err != nil {
		return
	}
	hashC := base64.StdEncoding.EncodeToString(check.Sum(nil))
	if hashC != hash {
		return fmt.Errorf("hash inconsistency && hash is %s", hashC)
	}
	oid := strings.Join([]string{bucketName, objectName}, ".")
	err = RadosMgr.Rados.WriteObject(BucketData, oid, data, 0)
	if err != nil {
		return
	}
	defer rollback(func() {
		go func() {
			for {
				err := RadosMgr.Rados.DeleteObject(BucketData, oid)
				if err == nil {
					break
				}
			}
		}()
	})

	metadata, err := json.Marshal(&metadataM)
	if err != nil {
		return
	}
	metadataId := oid + "-metadata"
	err = RedisMgr.Redis.SetDataByString(metadataId, string(metadata))
	if err != nil {
		return err
	}
	return nil
}

func GetObject(bucketName, objectName string) ([]byte, error) {
	oid := strings.Join([]string{bucketName, objectName}, ",")
	var data []byte
	datacache := make([]byte, 1024*1024)
	var offset uint64 = 0
	for {
		n, err := RadosMgr.Rados.ReadObject(BucketData, oid, datacache, offset)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		data = append(data, datacache[:n]...)
		offset = uint64(n)
	}
	return data, nil
}
