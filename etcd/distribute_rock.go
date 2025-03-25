package etcd

import (
	"errors"
	"strconv"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/distribute"

	recipes "github.com/bovinae/etcd-recipes"
)

const (
	// KQueuePrefixDigRule      = "dig_rule"
	KQueuePrefixExecRule     = "exec_rule"
	KQueuePrefixUpdateYValue = "update_y_val"
)

func Distribute(prefix string, taskId, ruleId int64, tableId string, index int) (*recipes.Queue, error) {
	etcdKey := distribute.EtcdKey{
		KeyPrefix: distribute.KEtcdKeyPrefix,
		UserId:    distribute.GEtcd.TaskKey.UserId,
		KeyType:   distribute.KEtcdKeyTypeQueue,
		BizType:   prefix,
		KeySuffix: strconv.FormatInt(taskId, 10) + "_" + tableId,
	}
	if ruleId > 0 {
		etcdKey.KeySuffix += distribute.KSplitter + strconv.FormatInt(ruleId, 10)
	}
	key := etcdKey.String()
	queue, err := distribute.NewQueue(key, func(q *recipes.Queue) error {
		for i := 0; i < index; i++ {
			if err := q.Enqueue(strconv.Itoa(i)); err != nil {
				logger.Errorf("Enqueue to etcd failed, err=%v, i=%v", err, i)
				if _, err := q.Delete(); err != nil {
					// queue name add epoch to prevent the impact of Delete error
					logger.Errorf("Delete queue failed, err=%v, taskId=%v, tableId=%v", err, taskId, tableId)
				}
				return err
			}
		}
		logger.Info("Enqueue to etcd success, total index=", index, " taskId=", taskId, " ruleId=", ruleId, " tableId=", tableId)
		return nil
	})
	if err != nil {
		logger.Errorf("NewQueue failed, err=%v, key=%v", err, key)
		return nil, err
	}

	return queue, nil
}

func Dequeue(queue *recipes.Queue, maxIndex int, referer string) (int, error) {
	// time.Sleep(time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10)) * time.Second)
	index, err := queue.Dequeue(false)
	if err != nil {
		if err != recipes.ErrEmptyQueue {
			logger.Errorf("queue.Dequeue failed, err=%v", err)
		}
		return 0, err
	}
	i, err := strconv.Atoi(index)
	if err != nil {
		logger.Errorf("strconv.Atoi failed, err=%v, index=%v", err, index)
		return 0, err
	}
	if i < 0 || i >= maxIndex {
		logger.Errorf("invalid index, i=%v", i)
		return 0, errors.New("invalid index")
	}
	logger.Infof("for test %v i: %v", referer, i)
	return i, nil
}

// func ShouldFindRegularRule(taskId int64, tableId string) bool {
// 	key := strconv.FormatInt(taskId, 10) + "_" + tableId
// 	cli, err := distribute.GetEtcdClient()
// 	if err != nil {
// 		logger.Errorf("GetEtcdClient failed, err=%v", err)
// 		return true
// 	}
// 	dlock := distribute.NewDistributeLock(cli, distribute.GEtcd.Jncn.UserId, "regular")
// 	dlock.Lock(key)
// 	defer dlock.Unlock()
// 	val, err := distribute.Get(getRegularRuleFindKey(key))
// 	if err != nil {
// 		logger.Errorf("Get from etcd failed, err=%v, key=%v", err, key)
// 		return true
// 	}
// 	if val == "1" {
// 		return false
// 	}
// 	if err := distribute.PutWithTTL(getRegularRuleFindKey(key), "1", 1800); err != nil {
// 		logger.Errorf("PutWithTTL to etcd failed, err=%v, key=%v", err, key)
// 	}
// 	return true
// }

// func getRegularRuleFindKey(key string) string {
// 	return distribute.EtcdKey{
// 		KeyPrefix: distribute.KEtcdKeyPrefix,
// 		UserId:    distribute.GEtcd.Jncn.UserId,
// 		KeyType:   distribute.KEtcdKeyTypeRule,
// 		BizType:   "regular",
// 		KeySuffix: key,
// 	}.String()
// }
