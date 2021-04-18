package connection

type RadosManager struct {
	Rados *Rados
}

type RedisManager struct {
	Redis *Redis
}

var RedisMgr *RedisManager
var RadosMgr *RadosManager

func InitRedisManager(r *Redis) {
	RedisMgr = &RedisManager{
		Redis: r,
	}
}

func InitRadosManager(r *Rados) {
	RadosMgr = &RadosManager{
		Rados: r,
	}
}
