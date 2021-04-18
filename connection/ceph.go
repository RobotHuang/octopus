package connection

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"math"
	"runtime"
	"time"
)

type Rados struct {
	Conn  *rados.Conn
	Pools map[string]bool
}

func NewRados() (*Rados, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, err
	}
	ceph := &Rados{
		Conn:  conn,
		Pools: make(map[string]bool),
	}

	runtime.SetFinalizer(ceph, finalizerRados)
	return ceph, nil
}

func NewRadosWithArgs(user, monitors, keyring string) (*Rados, error) {
	conn, err := rados.NewConnWithUser(user)
	if err != nil {
		err = fmt.Errorf("NewConn failed")
		return nil, err
	}
	keyFile := fmt.Sprintf("--keyFile=%v", keyring)
	args := []string{"-m", monitors, keyFile}
	err = conn.ParseCmdLineArgs(args)
	if err != nil {
		err = fmt.Errorf("ParseCmdLineArgs failed")
		return nil, err
	}
	r := &Rados{
		Conn:  conn,
		Pools: make(map[string]bool),
	}
	return r, nil
}

func (r *Rados) InitDefault() error {
	err := r.Conn.ReadDefaultConfigFile()
	if err != nil {
		return err
	}
	ch := make(chan error)
	go func() {
		ch <- r.Conn.Connect()
	}()
	select {
	case err = <-ch:
	case <-time.After(time.Second * 5):
		err = fmt.Errorf("timed out waiting for connect")
	}
	if err != nil {
		return err
	}
	fmt.Println("connect ceph cluster successfully")
	err = r.InitPools()
	if err != nil {
		return err
	}

	return nil
}

const (
	// rgw.bucket.data stores the object
	BucketData = "rgw.bucket.data"
)

// InitPools creates the pools the go-rgw needs
func (r *Rados) InitPools() error {
	existedPools, err := r.Conn.ListPools()
	if err != nil {
		return err
	}
	// determine whether pools already have existed
	for _, value := range existedPools {
		if value == BucketData {
			r.Pools[BucketData] = true
		}
	}
	// if pool doesn't exist, create the pool.
	if _, ok := r.Pools[BucketData]; !ok || !r.Pools[BucketData] {
		err := r.createPool(BucketData)
		if err != nil {
			return err
		}
		r.Pools[BucketData] = true
	}
	return nil
}

// create pool
func (r *Rados) createPool(name string) error {
	err := r.Conn.MakePool(name)
	return err
}

// Shutdown close the connection to the Rados cluster
func finalizerRados(r *Rados) {
	if r.Conn != nil {
		r.Conn.Shutdown()
	}
}

func (r *Rados) WriteObject(pool string, oid string, data []byte, offset uint64) error {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.Write(oid, data, offset)
	if err != nil {
		return err
	}
	return nil
}

func (r *Rados) ReadObject(pool string, oid string, data []byte, offset uint64) (int, error) {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return 0, err
	}
	defer ioctx.Destroy()
	num, err := ioctx.Read(oid, data, offset)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (r *Rados) DeleteObject(pool string, oid string) error {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	return ioctx.Delete(oid)
}

func (r *Rados) GetXattr(pool string, oid string, name string, data []byte) (int, error) {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return 0, err
	}
	defer ioctx.Destroy()
	num, err := ioctx.GetXattr(oid, name, data)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (r *Rados) SetXattr(pool string, oid string, name string, data []byte) error {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.SetXattr(oid, name, data)
	if err != nil {
		return err
	}
	return nil
}

func (r *Rados) GetOmap(pool string, oid string) (map[string][]byte, error) {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()
	m, err := ioctx.GetOmapValues(oid, "", "", math.MaxInt64)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (r *Rados) SetOmap(pool string, oid string, pairs map[string][]byte) error {
	ioctx, err := r.Conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	err = ioctx.SetOmap(oid, pairs)
	return err
}
