package main

import (
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"octopus/connection"
	"octopus/router"
)

var (
	cacheFlag bool
)

func main() {
	cacheFlag = *(flag.Bool("cache", false, "Cache enable or not"))
	var re = connection.NewRedis("tcp", "127.0.0.1:6379", "")
	var err = re.Init()
	if err != nil {
		fmt.Println(err)
		return
	}
	r, err := connection.NewRados()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := r.InitDefault(); err != nil {
		fmt.Println(err)
		return
	}
	var g *gin.Engine
	g = router.SetRouter()
	if err := g.Run(":8080"); err != nil {
		fmt.Println(err)
		return
	}
}
