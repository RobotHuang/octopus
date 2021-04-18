package router

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"octopus/session"
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

}

func getObject(c *gin.Context) {

}
