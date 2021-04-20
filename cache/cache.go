package cache

type FileChunk struct {
	Size int
	Capacity int
	Objects []*ObjectChunk
}

type LRUCache struct {
	size int
	capacity int
	cache map[string]*DLinkedNode
	head, tail *DLinkedNode
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
	Data []byte
}

func newDLinkedNode(object *ObjectChunk) *DLinkedNode {
	return &DLinkedNode{
		object: object,
		prev: nil,
		next: nil,
	}
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		size: 0,
		capacity: capacity,
		cache: make(map[string]*DLinkedNode),
		head: newDLinkedNode(nil),
		tail: newDLinkedNode(nil),
	}
}