package etcd

import (
	"context"
	"errors"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_cluster"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// ResponseTimeout 相应超时时间
const ResponseTimeout = 600 * time.Second

// DialTimeout 拨号超时时间
const DialTimeout = time.Second * 5

var GlobalEtcd *Etcd

type Etcd struct {
	client *clientv3.Client
}

func (e *Etcd) GetClient() *clientv3.Client {
	return e.client
}

func NewEtcd(cluster []string) (*Etcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cluster,
		DialTimeout: ResponseTimeout,
	})
	if err != nil {
		return nil, err
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for {
		select {
		case <-timeoutCtx.Done():
			return nil, errors.New("etcd connection timed out")
		case <-time.After(time.Second):
			checkTime, cancel_ := context.WithTimeout(context.Background(), time.Second)
			_, err = cli.Status(checkTime, cluster[0])
			cancel_()
			if err == nil {
				return &Etcd{cli}, nil
			}
		}
	}

}

func (e *Etcd) Get(key string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, err := e.client.Get(ctx, key)
	cancel()
	return resp, err
}

func (e *Etcd) GetPrefix(topic string, addNode func(key string, value string)) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, _ := e.client.Get(ctx, topic, clientv3.WithPrefix())
	cancel()

	if resp != nil {
		for _, ev := range resp.Kvs {
			// 监听到Put事件，添加节点到本地哈希环中 todo
			addNode(string(ev.Key), string(ev.Value))
		}
	}
}

func (e *Etcd) Put(key string, value string) (*clientv3.PutResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, err := e.client.Put(ctx, key, value)
	cancel()
	return resp, err
}

func (e *Etcd) Delete(key string) (*clientv3.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	delResp, err := e.client.Delete(ctx, key)
	cancel()
	return delResp, err
}

func (e *Etcd) DeleteWithPrefix(topic string) (*clientv3.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	delResp, err := e.client.Delete(ctx, topic, clientv3.WithPrefix())
	cancel()
	return delResp, err
}

func (e *Etcd) Watch(key string, function func(event *clientv3.Event)) {
	rch := e.client.Watch(context.Background(), key)
	for watchResp := range rch {
		for _, event := range watchResp.Events {
			go function(event)
		}
	}
}

func (e *Etcd) RegisterNode(key string, value string) {
	_, err := e.Put(key, value)
	if err != nil {
		logger.Errorf("put to etcd failed: %v", err)
	}
	return
}

func (e *Etcd) WatchNode(topic string, addNode func(key string, value string)) {
	watcher := clientv3.NewWatcher(e.client)
	watchChan := watcher.Watch(context.TODO(), topic, clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				addNode(string(event.Kv.Key), string(event.Kv.Value))
			}
		}
	}
}

type NodeOp int

func (op NodeOp) String() string {
	switch op {
	case Add:
		return "add"
	case Remove:
		return "remove"
	default:
		return "unexpected op!"
	}
}

const (
	Add    NodeOp = 0
	Remove NodeOp = 1
)

func (e *Etcd) WatchNodeEvent(topic string, updateNode func(key string, value string, op NodeOp)) {
	//watcher := clientv3.NewWatcher(e.client)
	//defer watcher.Close()
	watchChan := e.client.Watch(context.TODO(), topic, clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				updateNode(string(event.Kv.Key), string(event.Kv.Value), Add)
			case mvccpb.DELETE:
				updateNode(string(event.Kv.Key), string(event.Kv.Value), Remove)
			}
		}
	}
}

func (e *Etcd) GetNodeInfoWithPrefix(topic string, updateNode func(key string, value string, op NodeOp)) {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, _ := e.client.Get(ctx, topic, clientv3.WithPrefix())
	cancel()

	if resp != nil {
		for _, ev := range resp.Kvs {
			// 监听到Put事件，添加节点到本地哈希环中 todo
			updateNode(string(ev.Key), string(ev.Value), Add)
		}
	}
}

func (e *Etcd) GetKVWithPrefix(topic string) []*mvccpb.KeyValue {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, _ := e.client.Get(ctx, topic, clientv3.WithPrefix())
	cancel()

	if resp != nil {
		return resp.Kvs
	}
	return nil
}

func (e *Etcd) JudgeEndStatusWithPrefix(topic string, machineNum int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), ResponseTimeout)
	resp, _ := e.client.Get(ctx, topic, clientv3.WithPrefix())
	cancel()
	if len(resp.Kvs) == machineNum {
		return true
	}
	return false
}

func GetEndpoints() []string {
	return []string{conf_cluster.Cluster.EtcdAddr}
}
