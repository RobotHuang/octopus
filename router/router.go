package router

import "github.com/gin-gonic/gin"

func SetRouter() *gin.Engine {
	g := gin.Default()
	g.GET("/createbucket/:bucket", createBucket)
	g.GET("listbucket", listBuckets)

	g.POST("/upload/:bucket/:object", putObject)
	g.GET("/download/:bucket/:object", getObject)

	return g
}
