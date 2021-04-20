package cache

import (
	"encoding/json"
	. "octopus/connection"
	"octopus/util"
)

// 2-level cache
// 1st cache is LRUCache
// 2nd cache is FileChunk

type FileChunk struct {
	Size int
	Capacity int
	Objects []*ObjectChunk
}

type LRUCache struct {
	size int
	capacity int
	// key is oid
	cache map[string]*DLinkedNode
	head, tail *DLinkedNode
	fileChunk *FileChunk
}

type DLinkedNode struct {
	object *ObjectChunk
	prev, next *DLinkedNode
}

type ObjectChunk struct {
	ObjectId string
	Offset int
	Size int
	ETag string
	Metadata string
	Data []byte
}

type ObjectInfo struct {
	ObjectId string `json:"objectId"`
	Offset int `json:"offset"`
	Size int `json:"size"`
	ETag string `json:"etag"`
	Metadata string `json:"metadata"`
}

func newDLinkedNode(object *ObjectChunk) *DLinkedNode {
	return &DLinkedNode{
		object: object,
		prev: nil,
		next: nil,
	}
}

func newObjectChunk(oid string, data []byte) *ObjectChunk {
	return &ObjectChunk{
		ObjectId: oid,
		Data: data,
		Size: len(data),
	}
}

func NewLRUCache(capacity int, chunkCapacity int) *LRUCache {
	return &LRUCache{
		size: 0,
		capacity: capacity,
		cache: make(map[string]*DLinkedNode),
		head: newDLinkedNode(nil),
		tail: newDLinkedNode(nil),
		fileChunk: &FileChunk{
			Capacity: chunkCapacity,
		},
	}
}

func (l *LRUCache) Get(oid string) []byte {
	if _, ok := l.cache[oid]; !ok {
		return l.fileChunk.GetFromChunk(oid)
	}
	node := l.cache[oid]
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

func (l *LRUCache) Put(oid string, data []byte) {
	if _, ok := l.cache[oid]; !ok {
		node := newDLinkedNode(newObjectChunk(oid, data))
		l.cache[oid] = node
		l.addToHead(node)
		l.size++
		if l.size > l.capacity {
			removed := l.removeTail()
			l.fileChunk.mergeToChunk(removed.object)
			l.size--
		}
	} else {
		node := l.cache[oid]
		node.object.Data = data
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
		err = RedisMgr.Redis.SetDataByString(object.ObjectId + "-metadata", string(objectInfoByte))
		if err != nil {
			return err
		}
	}
	return nil
}