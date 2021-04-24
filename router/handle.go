package router

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"octopus/session"
	"strings"
)

const metaPrefix = "C-Meta-"
const acl = "C-Acl"

func createBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	bucketAcl := c.GetHeader(acl)
	err := session.CreateBucket(bucketName, string(bucketAcl))
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	c.Status(http.StatusOK)
}

func listBuckets(c *gin.Context) {
	buckets, err := session.ListBuckets()
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	c.JSON(http.StatusOK, buckets)
}

func putObject(c *gin.Context) {
	body := c.Request.Body
	hash := c.GetHeader("Content-MD5")
	var metadata = make(map[string][]string)
	for key, value := range c.Request.Header {
		if strings.HasPrefix(key, metaPrefix) {
			metadata[key] = value
		}
	}
	// add hash to the map of metadata
	var hashs []string
	hashs = append(hashs, hash)
	metadata["Content-MD5"] = hashs
	bucketName := c.Param("bucket")
	objectName := c.Param("object")
	//err := session.PutObject(bucketName, objectName, body, hash, metadata)
	err := session.PutObjectWithCache(bucketName, objectName, body, hash, metadata)
	if err != nil {
		c.String(http.StatusInternalServerError, fmt.Sprintf("%v", err))
		return
	}
	// return the hash of object
	c.Header("ETag", hash)
	c.Status(http.StatusOK)
}

func getObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectName := c.Param("object")
	//content, err := session.GetObject(bucketName, objectName)
	content, err := session.GetObjectWithCache(bucketName, objectName)
	if err == nil {
		c.Writer.WriteHeader(http.StatusOK)
		c.Header("Content-Disposition", "attachment; filename="+objectName)
		c.Header("Content-Type", "application/text/plain")
		c.Header("Accept-Length", fmt.Sprintf("%d", len(content)))
		_, _ = c.Writer.Write(content)
	} else {
		c.Status(http.StatusInternalServerError)
	}
}
