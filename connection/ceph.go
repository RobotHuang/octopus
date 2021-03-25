package connection

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"runtime"
	"time"
)

type Ceph struct {
	Conn *rados.Conn
	Pools map[string]bool
}

func NewCeph() (*Ceph, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, err
	}
	ceph := &Ceph{
		Conn: conn,
		Pools: make(map[string]bool),
	}

	runtime.SetFinalizer(ceph, finalizerCeph)
	return ceph, nil
}

func NewCephWithArgs(user, monitors, keyring string) (*Ceph, error) {
	conn, err := rados.NewConnWithUser(user)
	if err != nil {
		err = fmt.Errorf("NewConn failed")
		return nil, err
	}
	keyfile := fmt.Sprintf("--keyfile=%v", keyring)
	args := []string{"-m", monitors, keyfile}
	err = conn.ParseCmdLineArgs(args)
	if err != nil {
		err = fmt.Errorf("ParseCmdLineArgs failed")
		return nil, err
	}
	ceph := &Ceph{
		Conn: conn,
		Pools: make(map[string]bool),
	}
	return ceph, nil
}

func (c *Ceph) InitDefault() error {
	err := c.Conn.ReadDefaultConfigFile()
	if err != nil {
		return err
	}
	ch := make(chan error)
	go func() {
		ch <- c.Conn.Connect()
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
	err = c.InitPools()
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
func (c *Ceph) InitPools() error {
	existedPools, err := c.Conn.ListPools()
	if err != nil {
		return err
	}
	// determine whether pools already have existed
	for _, value := range existedPools {
		if value == BucketData {
			c.Pools[BucketData] = true
		}
	}
	// if pool doesn't exist, create the pool.
	if _, ok := c.Pools[BucketData]; !ok || !c.Pools[BucketData] {
		err := c.createPool(BucketData)
		if err != nil {
			return err
		}
		c.Pools[BucketData] = true
	}
	return nil
}

// create pool
func (c *Ceph) createPool(name string) error {
	err := c.Conn.MakePool(name)
	return err
}

// Shutdown close the connection to the Ceph cluster
func finalizerCeph(c *Ceph) {
	if c.Conn != nil {
		c.Conn.Shutdown()
	}
}

func (c *Ceph) WriteObject(pool string, oid string, data []byte, offset uint64) error {
	ioctx, err := c.Conn.OpenIOContext(pool)
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

func (c *Ceph) ReadObject(pool string, oid string, data []byte, offset uint64) (int, error) {
	ioctx, err := c.Conn.OpenIOContext(pool)
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

func (c *Ceph) DeleteObject(pool string, oid string) error {
	ioctx, err := c.Conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()
	return ioctx.Delete(oid)
}