package connection

import (
	"github.com/gomodule/redigo/redis"
	"runtime"
)

type Redis struct {
	// redis ip address
	IPAddr string
	// network transportation protocol, like udp/tcp
	Network string
	Password string
	Conn redis.Conn
}

func finalizerRedis(redis *Redis) {
	_ = redis.Conn.Close()
}

func NewRedis(network, ipAddr, password string) *Redis {
	r := &Redis{IPAddr: ipAddr, Network: network, Password: password}
	runtime.SetFinalizer(r, finalizerRedis)
	return r
}

func (r *Redis) Init() error {
	//redis.Dial(r.Network, r.IPAddr)
	if r.Password == "" {
		conn, err := redis.Dial(r.Network, r.IPAddr)
		if err != nil {
			return err
		}
		r.Conn = conn
	} else {
		conn, err := redis.Dial(r.Network, r.IPAddr, redis.DialPassword(r.Password))
		if err != nil {
			return err
		}
		r.Conn = conn
	}
	return nil
}

func (r *Redis) PutMetadata(id, metadata string) error {
	_, err := r.Conn.Do("SET", id, metadata)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) GetMetadata(id string) (string, error) {
	reply, err := redis.String(r.Conn.Do("GET", id))
	if err != nil {
		return "", err
	}
	return reply, nil
}

func (r *Redis) DeleteMetadata(id string) error {
	_, err := r.Conn.Do("DEL", id)
	if err != nil {
		return err
	}
	return nil
}