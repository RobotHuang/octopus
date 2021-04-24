package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

const ip = "http://localhost:8080"

func main() {
	CreateBucket("bucket1")
	ListBucket()
	for i := 0; i < 7; i++ {
		PutObject("bucket1", "object"+strconv.Itoa(i)+"s", []byte("Hello World"))
	}
	for i := 0; i < 7; i++ {
		GetObject("bucket1", "object"+strconv.Itoa(i)+"s")
	}
	//GetObject("bucket1", "object"+strconv.Itoa(0)+"s")
}

func CreateBucket(bucket string) {
	client := http.Client{}
	request, err := http.NewRequest("GET", ip+"/createbucket/"+bucket, nil)
	if err != nil {
		fmt.Println(err)
	}
	rep, err := client.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%v\n", rep.StatusCode)
	//res, _ := ioutil.ReadAll(rep.Body)
	//fmt.Printf("%v", string(res))
	//_ = rep.Body.Close()
}

func ListBucket() {
	client := http.Client{}
	request, err := http.NewRequest("GET", ip+"/listbucket", nil)
	if err != nil {
		fmt.Println(err)
	}
	rep, err := client.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	res, _ := ioutil.ReadAll(rep.Body)
	fmt.Printf("%v\n", string(res))
	_ = rep.Body.Close()
}

func PutObject(bucketName, objectName string, data []byte) {
	client := http.Client{}
	body := bytes.NewReader(data)
	request, err := http.NewRequest("POST", ip+"/upload/"+bucketName+"/"+objectName, body)
	if err != nil {
		fmt.Println(err)
	}
	checkSum := md5.New()
	checkSum.Write(data)
	hash := base64.StdEncoding.EncodeToString(checkSum.Sum(nil))
	fmt.Println(hash)
	request.Header.Add("Content-MD5", hash)
	request.Header.Add("c-meta-hello", "hello meta")
	rep, err := client.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(rep.StatusCode)
	res, err := ioutil.ReadAll(rep.Body)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%v", string(res))
	_ = rep.Body.Close()
}

func GetObject(bucketName, objectName string) {
	client := http.Client{}
	request, _ := http.NewRequest("GET", ip+"/download/"+bucketName+"/"+objectName, nil)
	rep, _ := client.Do(request)
	data, _ := ioutil.ReadAll(rep.Body)
	fmt.Printf("%v\n", string(data))
	_ = rep.Body.Close()
}
