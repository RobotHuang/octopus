package session

import (
	. "octopus/connection"
	"octopus/util"
)

func CreateBucket(bucketName, bucketAcl string) (err error) {
	var rollback = func(rollback func()) {
		if err != nil {
			rollback()
		}
	}

	bucketId := util.GenerateRandStr(8)
	if err = RedisMgr.Redis.RPUSHData("bucket", bucketId); err != nil {
		return err
	}
	defer rollback(func() {
		_ = RedisMgr.Redis.LREMData("bucket", bucketId, 1)
	})
	if err = RedisMgr.Redis.SetDataByString(bucketId, bucketName); err != nil {
		return err
	}
	return nil
}

func ListBuckets() ([]string, error) {
	return RedisMgr.Redis.GetAllDataInList("bucket")
}
