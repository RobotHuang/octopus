package connection

import (
	"fmt"
	"gorm.io/driver/mysql"
	_ "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
)

type MySQL struct {
	User     string
	Password string
	IPAddr   string
	Name     string
	Charset  string
	Database *gorm.DB
	mutex    sync.RWMutex
}

type User struct {
	UserID   string `gorm:"primary_key"`
	Username string
	Password string
}

// TODO add foreign key
type Bucket struct {
	BucketId   string `gorm:"primary_key"`
	BucketName string
	UserId     string
}

// TODO add foreign key
// ObjectName = bucketID-object
// ObjectID = clustID.bucketID.ObjectUUID
// IsMultipart whether the object is a multipart upload
type Object struct {
	ObjectName  string `gorm:"primary_key"`
	ObjectID    string
	IsMultipart bool
}

// AclID is the objectID-acl
type ObjectACL struct {
	AclID string `gorm:"primary_key"`
	Acl   string
}

type BucketACL struct {
	AclID string `gorm:"primary_key"`
	Acl   string
}

// MetadataID is the objectID-metadata
type ObjectMetadata struct {
	MetadataID string `gorm:"primary_key"`
	Metadata   string
}

type ObjectPart struct {
	ObjectID     string `gorm:"primary_key;auto_increment:false"`
	PartID       string `gorm:"primary_key;auto_increment:false"`
	PartObjectID string
}

func NewMySQL(user, password, ipAddr, name, charset string) *MySQL {
	mysql := &MySQL{
		User:     user,
		Password: password,
		IPAddr:   ipAddr,
		Name:     name,
		Charset:  charset,
		Database: nil,
	}
	return mysql
}

func (m *MySQL) Init() error {
	dsn := fmt.Sprintf("%s:%s@(%s)/%s?charset=%s&parseTime=True&loc=Local", m.User, m.Password, m.IPAddr,
		m.Name, m.Charset)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	// TODO use the log
	fmt.Println("connect mysql successfully")

	// Auto migrate the database if these following tables are not in the database
	_ = db.AutoMigrate(&Bucket{})
	_ = db.AutoMigrate(&User{})
	_ = db.AutoMigrate(&Object{})
	_ = db.AutoMigrate(&ObjectACL{})
	_ = db.AutoMigrate(&ObjectMetadata{})
	_ = db.AutoMigrate(&BucketACL{})
	_ = db.AutoMigrate(&ObjectPart{})

	m.Database = db
	return nil
}

func (m *MySQL) Close() error {
	return nil
}

func (m *MySQL) CreateBucket(userId, name, id string) {
	bucket := Bucket{id, name, userId}
	m.Database.Create(&bucket)
}

func (m *MySQL) DeleteBucket(name string) {
	m.Database.Where("bucket_name = ?", name).Delete(&Bucket{})
}

func (m *MySQL) FindBucket(name string) (bucket Bucket) {
	m.Database.Where("bucket_name = ?", name).First(&bucket)
	return
}

func (m *MySQL) ListBuckets(uid string) []Bucket {
	var buckets []Bucket
	m.Database.Where("user_id = ?", uid).Find(&buckets)
	return buckets
}

func (m *MySQL) CreateBucketTransaction(userId, name, id, acl string) (err error) {
	tx := m.Database.Begin()

	defer func() {
		if err != nil && tx != nil {
			tx.Rollback()
		}
	}()

	bucket := Bucket{BucketId: id, BucketName: name, UserId: userId}
	if err = m.Database.Create(&bucket).Error; err != nil {
		return
	}
	bucketAcl := BucketACL{AclID: id + "-acl", Acl: acl}
	if err = m.Database.Create(&bucketAcl).Error; err != nil {
		return
	}

	tx.Commit()
	return
}

func (m *MySQL) CreateUser(username, password, uid string) {
	user := User{UserID: uid, Username: username, Password: password}
	m.Database.Create(&user)
}

func (m *MySQL) UpdateUsername(uid, username string) {
	m.Database.Model(&User{}).Where("user_id = ?", uid).Update("username", username)
}

func (m *MySQL) UpdatePassword(username, password string) {
	m.Database.Model(&User{}).Where("username = ?", username).Update("password", password)
}

func (m *MySQL) FindUser(username string) User {
	var u User
	m.Database.Where("username = ?", username).First(&u)
	return u
}

func (m *MySQL) CreateObject(objectName string, oid string, isMultipart bool) error {
	object := Object{ObjectName: objectName, ObjectID: oid, IsMultipart: isMultipart}
	return m.Database.Create(&object).Error
}

func (m *MySQL) DeleteObject(objectName string) error {
	return m.Database.Where("object_name = ?", objectName).Delete(Object{}).Error
}

func (m *MySQL) FindObject(objectName string) Object {
	var object Object
	m.Database.Where("object_name = ?", objectName).First(&object)
	return object
}

func (m *MySQL) UpdateObject(objectName, oid string) {
	m.Database.Model(&Object{}).Where("object_name = ?", objectName).Update("object_id", oid)
}

func (m *MySQL) DeleteObjectMetadata(metaID string) error {
	return m.Database.Where("metadata_id = ?", metaID).Delete(&ObjectMetadata{}).Error
}

func (m *MySQL) DeleteObjectAcl(aclID string) error {
	return m.Database.Where("acl_id = ?", aclID).Delete(&ObjectACL{}).Error
}

// save the acl, metadata and oid
func (m *MySQL) SaveObjectTransaction(objectName string, oid string, metadata string, acl string, isMultipart bool) (err error) {
	tx := m.Database.Begin()

	// rollback
	defer func() {
		if err != nil && tx != nil {
			tx.Rollback()
		}
	}()

	// get metadataId and aclId
	metadataID := oid + "-metadata"
	aclID := oid + "-acl"

	var tempObj Object
	// if the object has existed, update the objectId, metadata and acl,
	// or create the object.
	if tx.Where("object_name = ?", objectName).First(&tempObj); tempObj == (Object{}) {
		object := Object{ObjectName: objectName, ObjectID: oid, IsMultipart: isMultipart}
		if err = tx.Create(&object).Error; err != nil {
			return
		}
	} else {
		// delete metadata's and acl's old version
		tempMetadata := tempObj.ObjectID + "-metadata"
		if err = tx.Where("metadata_id = ?", tempMetadata).Delete(&ObjectMetadata{}).Error; err != nil {
			return
		}
		tempACL := tempObj.ObjectID + "-acl"
		if err = tx.Where("acl_id = ?", tempACL).Delete(&ObjectACL{}).Error; err != nil {
			return
		}
		tempObj.ObjectID = oid
		if err = tx.Save(&tempObj).Error; err != nil {
			return
		}
	}

	// save metadata
	objectMetadata := ObjectMetadata{MetadataID: metadataID, Metadata: metadata}
	if err = tx.Create(&objectMetadata).Error; err != nil {
		return
	}

	// save acl
	objectACL := ObjectACL{AclID: aclID, Acl: acl}
	if err = tx.Create(&objectACL).Error; err != nil {
		return
	}

	tx.Commit()
	return nil
}

// save multipartObject && metadata
func (m *MySQL) SavePartObjectTransaction(partObjectName, partObjectID, metadata string) (err error) {
	tx := m.Database.Begin()

	defer func() {
		if err != nil && tx != nil {
			tx.Rollback()
		}
	}()

	partObject := Object{ObjectName: partObjectName, ObjectID: partObjectID, IsMultipart: false}
	if err = tx.Create(&partObject).Error; err != nil {
		return
	}
	metadataID := partObjectID + "-metadata"
	objectMetadata := ObjectMetadata{MetadataID: metadataID, Metadata: metadata}
	if err = tx.Create(&objectMetadata).Error; err != nil {
		return
	}

	tx.Commit()
	return nil
}

func (m *MySQL) SaveObjectPartBatch(objectID string, parts map[string]string) error {
	sql := "INSERT INTO `object_parts` (`object_id`, `part_id`, `part_object_id`) VALUES"
	count := 0
	length := len(parts) - 1
	for key, value := range parts {
		if length == count {
			sql += fmt.Sprintf("('%s', '%s', '%s');", objectID, key, value)
		} else {
			sql += fmt.Sprintf("('%s', '%s', '%s'),", objectID, key, value)
			count++
		}
	}
	return m.Database.Exec(sql).Error
}

func (m *MySQL) FindObjectPart(objectID string) []ObjectPart {
	var objectParts []ObjectPart
	m.Database.Where("object_id = ?", objectID).Order("part_id asc").Find(&objectParts)
	return objectParts
}

func (m *MySQL) FindBukcetAcl(bucketAclID string) BucketACL {
	var bucketAcl BucketACL
	m.Database.Where("acl_id = ?", bucketAclID).First(&bucketAcl)
	return bucketAcl
}

func (m *MySQL) FindObjectAcl(objectAclID string) ObjectACL {
	var objectAcl ObjectACL
	m.Database.Where("acl_id = ?", objectAclID).First(&objectAcl)
	return objectAcl
}
