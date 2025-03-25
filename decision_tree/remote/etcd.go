package remote

import (
	"context"
	"errors"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type EtcdClient struct {
	cli *clientv3.Client

	// fixme:暂时不要加lease
	//lease clientv3.Lease
	//ttlMap map[int64]clientv3.LeaseID
}

func NewEtcdClient(endPoints []string) *EtcdClient {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: 4 * time.Second,
	})
	if err != nil {
		logger.Errorf("error when new etcd client: %v", err)
		return nil
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for {
		select {
		case <-timeoutCtx.Done():
			logger.Error("etcd connection timed out")
			return nil
		case <-time.After(time.Second):
			checkTime, cancel_ := context.WithTimeout(context.Background(), time.Second)
			_, err = cli.Status(checkTime, endPoints[0])
			cancel_()
			if err == nil {
				return &EtcdClient{
					cli: cli,
					//lease: clientv3.NewLease(cli),
					//ttlMap: make(map[int64]clientv3.LeaseID),
				}
			}
		}
	}
}

func (c *EtcdClient) Put(key, value string) (_err error) {
	_err = errors.New("put into etcd failed!! ")
	tryTime := 3
	for i := 0; i < tryTime; i++ {
		kv := clientv3.NewKV(c.cli)
		resp, err := kv.Put(context.TODO(), key, value)
		if err == nil {
			if resp.PrevKv != nil {
				logger.Warnf("key:value already exists! %s", resp.PrevKv.String())
			}
			_err = nil
			break
		} else {
			logger.Errorf("[etcd] put failed! %v on [%s -- %s]>>retry! %d", err, key, value, i)
			time.Sleep(4 * time.Second)
		}
	}
	return
}

func (c *EtcdClient) Get(key string) (res string, exists bool, _err error) {
	_err = errors.New("get from etcd failed!! ")
	tryTime := 3
	for i := 0; i < tryTime; i++ {
		kv := clientv3.NewKV(c.cli)
		resp, err := kv.Get(context.TODO(), key)
		if err == nil {
			exists = len(resp.Kvs) != 0
			if exists {
				res = string(resp.Kvs[0].Value)
			}
			_err = nil
			break
		} else {
			logger.Errorf("[etcd] get failed! %v on [%s]>>retry! %d", err, key, i)
			time.Sleep(4 * time.Second)
		}
	}
	return
}

func (c *EtcdClient) GetWithPrefix(prefix string) (res map[string]string, _err error) {
	_err = errors.New("get(with prefix) from etcd failed!! ")
	tryTime := 3
	for i := 0; i < tryTime; i++ {
		kv := clientv3.NewKV(c.cli)
		resp, err := kv.Get(context.TODO(), prefix, clientv3.WithPrefix())
		if err == nil {
			res = make(map[string]string, len(resp.Kvs))
			for _, kv := range resp.Kvs {
				res[string(kv.Key)] = string(kv.Value)
			}
			_err = nil
			break
		} else {
			logger.Errorf("[etcd] get with prefix failed! %v on [%s]>>retry! %d", err, prefix, i)
			time.Sleep(4 * time.Second)
		}
	}
	return
}

// Watch 只进行等待，不做任何操作
func (c *EtcdClient) Watch(ctx context.Context, key string) clientv3.WatchChan {
	return c.cli.Watch(ctx, key)
	//for _, event := range events.Events {
	//	switch event.Type {
	//	case mvccpb.PUT:
	//		fmt.Println("on put:", string(event.Kv.Key), string(event.Kv.Value), event.Kv.Lease)
	//	case mvccpb.DELETE:
	//		fmt.Println("on delete:", string(event.Kv.Key), string(event.Kv.Value), event.Kv.Lease)
	//	}
	//}
}

// WatchWithPrefix 只进行等待，不做任何操作
func (c *EtcdClient) WatchWithPrefix(ctx context.Context, prefix string) clientv3.WatchChan {
	return c.cli.Watch(ctx, prefix, clientv3.WithPrefix())
}

func (c *EtcdClient) Delete(key string) (err error) {
	tryTime := 3
	for i := 0; i < tryTime; i++ {
		kv := clientv3.NewKV(c.cli)
		_, err = kv.Delete(context.TODO(), key)
		if err == nil {
			break
		} else {
			logger.Errorf("[etcd] delete failed! %v on [%s]>>retry %d", err, key, i)
			time.Sleep(4 * time.Second)
		}
	}
	return
}

func (c *EtcdClient) DeleteWithPrefix(prefix string) (err error) {
	tryTime := 3
	for i := 0; i < tryTime; i++ {
		kv := clientv3.NewKV(c.cli)
		_, err = kv.Delete(context.TODO(), prefix, clientv3.WithPrefix())
		if err == nil {
			break
		} else {
			logger.Errorf("[etcd] delete with prefix failed! %v on [%s]>>retry! %d", err, prefix, i)
			time.Sleep(4 * time.Second)
		}
	}
	return
}

// Close 关闭
func (c *EtcdClient) Close() error {
	return c.cli.Close()
}
