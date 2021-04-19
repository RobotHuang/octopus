package main

// oid = bucketName.ObjectName
// metadataId = bucketName.ObjectName-metadata

// BucketInfo is bucket's information
type BucketInfo struct {
	BucketId   string `json:"bucketId"`
	BucketName string `json:"bucketName"`
}

// ObjectInfo is object's information
type ObjectInfo struct {
	// ObjectId clusterID.bucketID.objectUUID
	ObjectId   string
	ObjectName string
}

type ObjectMetadata struct {
	// MetadataId ObjectId-metadata
	MetadataId string
	Metadata   string
}
