package cache

import (
	"encoding/json"
	"fmt"
	. "octopus/connection"
	"octopus/util"
	"time"
)

// TODO: 2nd Method: When object is written to LRUCache, it also is written to FileChunk.

// 2-level cache
// 1st cache is LRUCache
// 2nd cache is FileChunk

var Cache *LRUCache

type FileChunk struct {
	Size     int
	Capacity int
	Objects  []*ObjectChunk
}

type LRUCache struct {
	size     int
	capacity int
	// key is oid
	// TODO Modify to map of thread safe
	cache      map[string]*DLinkedNode
	head, tail *DLinkedNode
	fileChunk  *FileChunk
}

type DLinkedNode struct {
	object     *ObjectChunk
	prev, next *DLinkedNode
}

type ObjectChunk struct {
	ObjectId string
	Offset   int
	Size     int
	ETag     string
	Metadata string
	Data     []byte
	Update   bool
	Written  bool
	Weight   int // When weight is below the threshold, this object would be deleted from cache.
}

type ObjectInfo struct {
	ParentId string `json:"parentId"`
	ObjectId string `json:"objectId"`
	Offset   int    `json:"offset"`
	Size     int    `json:"size"`
	ETag     string `json:"etag"`
	Metadata string `json:"metadata"`
}

func newDLinkedNode(object *ObjectChunk) *DLinkedNode {
	return &DLinkedNode{
		object: object,
		prev:   nil,
		next:   nil,
	}
}

func NewObjectChunk(oid string, metadata string, data []byte, update, written bool, weight int) *ObjectChunk {
	return &ObjectChunk{
		ObjectId: oid,
		Data:     data,
		Size:     len(data),
		Update:   update,
		Written:  written,
		Weight:   weight,
	}
}

// NewLRUCache new a LRU cache struct
// capacity is the number of cache
// chunkCapacity is the number of objects in one chunk
func NewLRUCache(capacity int, chunkCapacity int) *LRUCache {
	l := LRUCache{
		size:     0,
		capacity: capacity,
		cache:    make(map[string]*DLinkedNode),
		head:     newDLinkedNode(nil),
		tail:     newDLinkedNode(nil),
		fileChunk: &FileChunk{
			Capacity: chunkCapacity,
		},
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return &l
}

func InitCache(cache *LRUCache) {
	Cache = cache
	fmt.Println("Init Monitor...")
	go Cache.DeleteCacheMonitor()
}

func (l *LRUCache) DeleteCacheMonitor() {
	for {
		// TODO make sure thread safe
		for k, v := range l.cache {
			// Threshold 0
			if v.object.Weight < 0 {
				// free memory
				l.removeNode(v)
				if !v.object.Written || v.object.Update {
					l.fileChunk.mergeToChunk(v.object)
				}
				delete(l.cache, k)
				l.size--
				fmt.Println("Remove From Cache...", k)
			} else {
				v.object.Weight--
			}
		}
		// TODO modify sleep time
		time.Sleep(5 * time.Minute)
	}
}

func (l *LRUCache) Get(oid string) []byte {
	if _, ok := l.cache[oid]; !ok {
		return l.fileChunk.GetFromChunk(oid)
	}
	node, _ := l.cache[oid]
	node.object.Weight = 5
	l.moveToHead(node)
	return node.object.Data
}

func (f *FileChunk) GetFromChunk(oid string) []byte {
	for _, object := range f.Objects {
		if object.ObjectId == oid {
			return object.Data
		}
	}
	return nil
}

func (l *LRUCache) Put(oid string, objectChunk *ObjectChunk) {
	if _, ok := l.cache[oid]; !ok {
		node := newDLinkedNode(objectChunk)
		l.cache[oid] = node
		l.addToHead(node)
		l.size++
		if l.size > l.capacity {
			removed := l.removeTail()
			if !removed.object.Written || removed.object.Update {
				l.fileChunk.mergeToChunk(removed.object)
			}
			delete(l.cache, removed.object.ObjectId)
			l.size--
		}
	} else {
		node := l.cache[oid]
		node.object.Data = objectChunk.Data
		node.object.Update = true
		l.moveToHead(node)
	}
}

func (l *LRUCache) moveToHead(node *DLinkedNode) {
	l.removeNode(node)
	l.addToHead(node)
}

func (l *LRUCache) removeNode(node *DLinkedNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (l *LRUCache) addToHead(node *DLinkedNode) {
	node.prev = l.head
	node.next = l.head.next
	l.head.next.prev = node
	l.head.next = node
}

func (l *LRUCache) removeTail() *DLinkedNode {
	node := l.tail.prev
	l.removeNode(node)
	return node
}

func (f *FileChunk) mergeToChunk(object *ObjectChunk) {
	f.Objects = append(f.Objects, object)
	f.Size++
	if f.Size >= f.Capacity {
		_ = f.writeToRados()
	}
}

// write fileChunk to Rados
// In Redis
// objectId-metadata-s -> metadata, including object's size, offset and metadata
func (f *FileChunk) writeToRados() (err error) {
	var data []byte
	for _, object := range f.Objects {
		object.Offset = len(data)
		data = append(data, object.Data...)
	}
	oid := util.GenerateRandStr(8)
	err = RadosMgr.Rados.WriteObject(BucketData, oid, data, 0)
	if err != nil {
		return err
	}
	for _, object := range f.Objects {
		objectInfo := ObjectInfo{
			oid,
			object.ObjectId,
			object.Offset,
			object.Size,
			object.ETag,
			object.Metadata,
		}
		objectInfoByte, err := json.Marshal(objectInfo)
		if err != nil {
			return err
		}
		err = RedisMgr.Redis.SetDataByString(object.ObjectId+"-metadata-s", string(objectInfoByte))
		if err != nil {
			return err
		}
	}
	f.Size = 0
	f.Objects = f.Objects[:0]
	return nil
}
