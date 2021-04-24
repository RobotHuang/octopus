package connection

import (
	"github.com/gomodule/redigo/redis"
	"runtime"
)

type Redis struct {
	// redis ip address
	IPAddr string
	// network transportation protocol, like udp/tcp
	Network  string
	Password string
	Conn     redis.Conn
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

func (r *Redis) SetDataByString(key, value string) error {
	_, err := r.Conn.Do("SET", key, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) GetDataByString(key string) (string, error) {
	reply, err := redis.String(r.Conn.Do("GET", key))
	if err != nil {
		return "", err
	}
	return reply, nil
}

func (r *Redis) Delete(key string) error {
	_, err := r.Conn.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) RPUSHData(key string, value string) error {
	_, err := r.Conn.Do("RPUSH", key, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) RPOPData(key string) error {
	_, err := r.Conn.Do("RPOP", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) LREMData(key string, value string, count int) error {
	_, err := r.Conn.Do("LREM", key, count, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) GetAllDataInList(key string) ([]string, error) {
	n, err := redis.Int(r.Conn.Do("LLEN", key))
	if err != nil {
		return nil, err
	}
	reply, err := redis.ByteSlices(r.Conn.Do("LRANGE", key, 0, n))
	if err != nil {
		return nil, err
	}
	var result []string
	for _, v := range reply {
		result = append(result, string(v[:]))
	}
	return result, nil
}

func (r *Redis) ExistsKey(key string) (bool, error) {
	exists, err := redis.Bool(r.Conn.Do("EXISTS", key))
	return exists, err
}
