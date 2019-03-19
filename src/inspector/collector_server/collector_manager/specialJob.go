package collectorManager

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"inspector/collector_server/configure"
	"inspector/collector_server/model"
	"inspector/config"
	"inspector/heartbeat"
	"inspector/util"

	"github.com/golang/glog"
)

const (
	spJobBeginSleep         = 10                   // seconds
	handleTaskJobBeginSleep = spJobBeginSleep + 10 // seconds
	tryThreshold            = 10
	trySleep                = 1000 // ms, 1s
	defaultInterval         = 5

	globalMd5       = "global_md5"
	electLeaderName = string(util.InnerLeadingMark) + "elect_leader"
)

type copyInfo struct {
	lock                sync.Mutex
	taskListAliveServer []*heartbeat.NodeStatus // alive server info
	md5Map              map[string][]byte       // includes global-md5 and each service md5
}

type SpecialJob struct {
	cs                        config.ConfigInterface // config server
	hb                        *heartbeat.Heartbeat   // heartbeat
	startTime                 int64                  // start time, used to make callback sleep to element vibration at the beginning
	watcherMap                *sync.Map              // watcher register map
	taskMap                   map[string]interface{} // current collector manager job, job -> {Md5Name->md5, TaskDistributeName->taskList}
	service                   string                 // current service name
	cm                        *CollectorManager      // not own
	taskDistributeHandlerLock sync.Mutex             // lock in the taskDistributeHandler

	// special job watcher
	taskListWatcher  *config.Watcher
	taskListCopyInfo *copyInfo // store the copy info generated from previous handle

	// task watcher
	taskDistributeWatcher *config.Watcher

	hbStartMark bool // mark just start
}

func NewSpecialJob(cm *CollectorManager) *SpecialJob {
	sj := &SpecialJob{
		cs:         cm.Cs,
		hb:         cm.Hb,
		startTime:  time.Now().Unix(),
		watcherMap: new(sync.Map),
		taskMap:    make(map[string]interface{}),
		service:    cm.HbConf.Service,
		cm:         cm,
		taskListCopyInfo: &copyInfo{
			taskListAliveServer: make([]*heartbeat.NodeStatus, 0),
			md5Map: map[string][]byte{
				globalMd5: {},
			},
		},
	}

	sj.taskListWatcher = &config.Watcher{
		Event:   config.NODEALL,
		Handler: sj.taskListHandler,
	}

	sj.taskDistributeWatcher = &config.Watcher{
		Event:   config.NODEALL,
		Handler: sj.taskDistributeHandler,
	}
	return sj
}

func (sj *SpecialJob) Start() error {
	// 1. register heartbeat watcher
	hbWatcher := &heartbeat.Watcher{
		Event:   heartbeat.WatcherAll,
		Handler: sj.heartbeatHandler,
	}
	if err := sj.hb.RegisterGlobalWatcher(heartbeat.ModuleCollector, heartbeat.ServiceAll, hbWatcher); err != nil {
		return err
	}

	// 2. register meta watcher
	metaWatcherAll := &config.Watcher{
		Event:   config.NODEALL,
		Handler: sj.metaHandler,
	}
	// if err := sj.cs.RegisterGlobalWatcher(SpecialJob, MetaName, metaWatcherAll); err != nil {
	// watch all documents in meta collection
	if err := sj.cs.RegisterGlobalWatcher(util.MetaCollection, "", metaWatcherAll); err != nil {
		return err
	}

	// 3. do handle task at the beginning
	// sj.handleTask()
	return nil
}

func (sj *SpecialJob) Stop() {
}

/*
 * watch meta collection changed.
 * pay attention: the sp_job must be removed first if corresponding item is removed in meta document
 */
func (sj *SpecialJob) metaHandler(event config.WatcheEvent) error {
	glog.Info("SpecialJob metaHandler called")

	//// iterate meta map
	//mp, err := sj.cs.GetMap(SpecialJob, MetaName)
	//if err != nil {
	//	glog.Errorf("metaHandler get meta list error[%v]", err)
	//	return err
	//}

	// iterate meta map
	keyLists, err := sj.cs.GetKeyList(util.MetaCollection)
	if err != nil {
		glog.Errorf("metaHandler get meta list error[%v]", err)
		return err
	}

	// convert keyLists to map for querying
	mp := make(map[string]struct{}, len(keyLists))

	// if element is not in the list, add watcher
	for _, ele := range keyLists {
		mp[ele] = struct{}{}
		if _, ok := sj.watcherMap.Load(ele); !ok {
			glog.Infof("SpecialJob metaHandler add service[%s]", ele)
			sj.watcherMap.Store(ele, struct{}{})

			// 1. register special job watcher
			// if err := sj.cs.RegisterGlobalWatcher(SpecialJob, ele, sj.taskListWatcher); err != nil {
			if err := sj.cs.RegisterGlobalWatcher(util.TaskListCollection, ele, sj.taskListWatcher); err != nil {
				glog.Errorf("metaHandler add taskListCollection watcher[%s] error[%v]", ele, err)
				return err
			}

			// 2. register task watcher
			if err := sj.cs.RegisterGlobalWatcher(util.TaskDistributeCollection, ele, sj.taskDistributeWatcher); err != nil {
				glog.Errorf("metaHandler add TaskDistributeCollection watcher[%s] error[%v]", ele, err)
				return err
			}
		}
	}

	// if element is removed in the remote meta
	sj.watcherMap.Range(func(k, v interface{}) bool {
		key := k.(string)
		if _, ok := mp[key]; !ok {
			glog.Infof("SpecialJob metaHandler remove service[%s]", key)
			// 1. remove special job watcher
			// if err := sj.cs.RemoveGlobalWatcher(SpecialJob, key); err != nil {
			if err := sj.cs.RemoveGlobalWatcher(util.TaskListCollection, key); err != nil {
				glog.Errorf("metaHandler remove taskListCollection watcher[%s] error[%v]", key, err)
				return true
			}

			// 2. remove task watcher
			if err := sj.cs.RemoveGlobalWatcher(util.TaskDistributeCollection, key); err != nil {
				glog.Errorf("metaHandler remove TaskDistributeCollection watcher[%s] error[%v]", key, err)
				return true
			}
			sj.watcherMap.Delete(key)
		}
		return true
	})

	return nil
}

func (sj *SpecialJob) heartbeatHandler(event heartbeat.WatchEvent) error {
	// handle task list change
	return sj.taskListHandler(config.UNKNOWN)
}

// special job list changed
func (sj *SpecialJob) taskListHandler(event config.WatcheEvent) error {
	// handle task distribute
	// wait {beginSleep} seconds to prevent fluctuations at beginning
	wait := time.Now().Unix() - sj.startTime
	if wait < spJobBeginSleep {
		// call only one time
		if sj.hbStartMark == false {
			sj.hbStartMark = true
			time.AfterFunc(time.Duration(spJobBeginSleep-wait)*time.Second, sj.taskListInnerHandler)
		}
		return nil
	}

	sj.taskListInnerHandler()
	return nil
}

// check md5sum first including global md5sum and each services md5. Concurrency is acceptable.
func (sj *SpecialJob) taskListInnerHandler() {
	glog.Info("taskListInnerHandler called")

	// quorum leader, only leader will run the below code
	if QuorumLeader(util.TaskListCollection, electLeaderName, conf.Options.CollectorServerAddress,
		sj.cs, sj.hb) == false {
		glog.Info("taskListInnerHandler called: I'm not the leader")
		return
	}

	glog.Info("taskListInnerHandler called: I'm the leader")

	// lock so that only one thread runs at the same time
	sj.taskListCopyInfo.lock.Lock()
	defer sj.taskListCopyInfo.lock.Unlock()

	// 1. get all alive services
	alive, err := sj.hb.GetServices(heartbeat.ModuleCollector, heartbeat.ServiceAlive)
	if err != nil {
		glog.Errorf("taskListInnerHandler: get alive services error[%v]", err)
		return
	}
	if len(alive) == 0 {
		glog.Errorf("taskListInnerHandler: get module[collector] alive services empty")
		return
	}
	aliveList := make([]string, len(alive))
	for i, e := range alive {
		aliveList[i] = e.Name
	}
	glog.Infof("taskListInnerHandler: alive module[collector] list: %v", aliveList)

	// 2. check if alive services == old alive services
	aliveChanged := false
	if len(alive) != len(sj.taskListCopyInfo.taskListAliveServer) {
		aliveChanged = true
	} else {
		for i := 0; i < len(alive); i++ {
			if alive[i].Name != sj.taskListCopyInfo.taskListAliveServer[i].Name ||
				alive[i].Gid != sj.taskListCopyInfo.taskListAliveServer[i].Gid {
				aliveChanged = true
				break
			}
		}
	}

	glog.Infof("taskListInnerHandler: alive services changed? [%v]", aliveChanged)

	// 3. check global md5 changed
	global, err := sj.cs.GetBytes(util.TaskListCollection, util.Md5Name)
	if err != nil {
		glog.Infof("taskListInnerHandler: get remote global md5 error[%v]", err)
		return
	}
	if aliveChanged || bytes.Compare(global, sj.taskListCopyInfo.md5Map[globalMd5]) != 0 {
		// check which keys are changed
		keyLists, err := sj.cs.GetKeyList(util.TaskListCollection)
		if err != nil {
			glog.Errorf("taskListInnerHandler: get key list error[%v]", err)
			return
		}

		changeKeys := make([]string, 0, len(keyLists))
		for _, key := range keyLists {
			md5, err := sj.cs.GetBytes(util.TaskListCollection, key, util.Md5Name)
			if err != nil && !util.IsNotFound(err) {
				glog.Errorf("taskListInnerHandler: get md5 of key[%s] error[%v]", key, err)
				continue
			}

			if v, ok := sj.taskListCopyInfo.md5Map[key]; !ok || bytes.Compare(v, md5) != 0 {
				changeKeys = append(changeKeys, key)
				sj.taskListCopyInfo.md5Map[key] = md5 // update local md5, if not found, md5 == empty
			}
		}

		// update local
		sj.taskListCopyInfo.taskListAliveServer = alive // update alive
		sj.taskListCopyInfo.md5Map[globalMd5] = global  // update global md5

		// run changed handler function
		if aliveChanged == false {
			sj.taskListInnerChangedHandler(alive, changeKeys)
		} else {
			sj.taskListInnerChangedHandler(alive, keyLists)
		}
	}
}

// taskList is changed
func (sj *SpecialJob) taskListInnerChangedHandler(alive []*heartbeat.NodeStatus, keyLists []string) {
	glog.Infof("taskListInnerChangedHandler changed called: keyLists: %v", keyLists)

	retryCallback := func() bool {
		// 1. construct job map: job(mongodb3.4, redis4.0, ...) -> instance list, every element is map
		jobMap := make(map[string][]map[string]interface{})
		for _, job := range keyLists {
			// fetch the map
			mp, err := sj.cs.GetMap(util.TaskListCollection, job, util.TaskDistributeName)
			if err != nil && !util.IsNotFound(err) {
				glog.Errorf("taskListInnerHandler: get job[%s] array error[%v]", job, err)
				return true
			}

			// get instance list
			instanceList := make([]map[string]interface{}, 0, len(mp))
			for key, val := range mp {
				if util.FilterName(key) {
					continue
				}
				instanceList = append(instanceList, val.(map[string]interface{}))
			}
			jobMap[job] = instanceList
		}

		// glog.V(2).Infof("taskListInnerHandler: jobMap[%v]", jobMap)

		// 2. calculate new task distribution
		// job -> gid -> task list
		for job, instanceList := range jobMap {
			// redundant store task information fetched from task-list collection,
			// key: collector service name(10.1.1.1:123), value: instance map
			distribution := make(map[string][]map[string]interface{})
			// calculate every task list
			for _, instance := range instanceList {
				var pid, hid int
				var err error
				if pid, err = util.ConvertInterface2Int(instance[model.PidName]); err != nil {
					pid = 0
				}
				if hid, err = util.ConvertInterface2Int(instance[model.HidName]); err != nil {
					glog.Warningf("taskListInnerHandler: job[%s] get host[%s]'s pid[%v] or hid[%v] error[%v]",
						job, instance[model.HostName], pid, hid, err)
					continue
				}
				id := util.HashInstance(uint32(pid), int32(hid), len(alive))
				serviceName := alive[id].Name
				distribution[serviceName] = append(distribution[serviceName], instance)
			}

			// get remote md5
			remoteMd5, err := sj.cs.GetBytes(util.TaskDistributeCollection, job, util.Md5Name)
			if err != nil && !util.IsNotFound(err) {
				glog.Errorf("taskListInnerHandler: job[%s] get remote md5 error[%v]", job, err)
				return false // no need to try again
			}

			// local md5
			md5 := sj.calTaskMd5(distribution)
			// glog.V(2).Info("fuck 2", job, md5, remoteMd5)
			// do compare
			if bytes.Compare(md5, remoteMd5) != 0 {
				glog.Infof("taskListInnerHandler: job[%s] remote md5[%v] != local md5[%v]",
					job, remoteMd5, md5)
				wholeMap := make(map[string]interface{})
				wholeMap[util.Md5Name] = md5
				wholeMap[util.TaskDistributeName] = distribution

				// because there is only one thread runs at the same time, so no need to lock and double check
				// lock
				//if err = sj.cs.Lock(TaskDistributeCollection, ""); err != nil {
				//	glog.Infof("taskListInnerHandler: job[%s] lock fail[%v]", job, err)
				//	needNextGenerate = true
				//	break
				//}
				//
				//// double check
				//remoteMd5Second, _ := sj.cs.GetBytes(TaskDistributeCollection, job, Md5Name)
				//if bytes.Compare(remoteMd5Second, remoteMd5) != 0 {
				//	glog.Info("taskListInnerHandler: job[%s] double check remote md5 updated, give up current job", job)
				//	sj.cs.Unlock(TaskDistributeCollection, "")
				//	// check next time?
				//	needNextGenerate = true
				//	continue
				//}

				// update remote
				if err = sj.cs.SetItem(util.TaskDistributeCollection, job, wholeMap); err != nil {
					glog.Errorf("taskListInnerHandler: job[%s] set map[%v] error[%v]", job, wholeMap, err)
					// sj.cs.Unlock(util.TaskDistributeCollection, "")
					return true
				}

				// unlock
				// sj.cs.Unlock(util.TaskDistributeCollection, "")
				glog.Infof("taskListInnerHandler: job[%s] update local successfully", job)
			}
			// glog.V(2).Info("fuck 3", job)
		}
		return false
	}

	if util.CallbackRetry(tryThreshold, trySleep, retryCallback) == false {
		glog.Errorf("taskListInnerHandler exceed threshold[%d], something wrong", tryThreshold)
	} else {
		glog.Info("taskListInnerHandler all succeed")
	}
}

// task changed
func (sj *SpecialJob) taskDistributeHandler(event config.WatcheEvent) error {
	glog.Info("SpecialJob taskDistributeHandler called")
	// 1. sleep awhile if just start
	wait := time.Now().Unix() - sj.startTime
	if wait < handleTaskJobBeginSleep {
		time.Sleep(time.Duration(handleTaskJobBeginSleep-wait) * time.Second)
	}

	sj.taskDistributeHandlerLock.Lock()
	defer sj.taskDistributeHandlerLock.Unlock()

	// 2. get meta map
	//metaMap, err := sj.cs.GetMap(SpecialJob, MetaName)
	//if err != nil {
	//	glog.Errorf("taskDistributeHandler get meta map error[%v]", err)
	//	return err
	//}

	// 2. get meta map
	keyList, err := sj.cs.GetKeyList(util.MetaCollection)
	if err != nil {
		glog.Errorf("taskDistributeHandler get meta key-list error[%v]", err)
		return err
	}

	// 3. diff remote(meta map) and local(task map)
	// judge whether added in the remote
	// taskMap: job -> instanceName -> instance; job -> md5 -> md5sum
	for _, job := range keyList {
		// get interval(step) and count
		// interval, count, err := sj.getBaseInfo(job)
		// if err != nil {
		// 	glog.Errorf("taskDistributeHandler: get base info with job[%s] error[%v]",
		// 		job, err)
		// 	return err
		// }
		var jobInfoMap map[string]interface{}
		var err error
		if jobInfoMap, err = sj.cs.GetMap(util.MetaCollection, job); err != nil {
			glog.Errorf("taskDistributeHandler: get job info with job[%s] error[%v]", job, err)
			return err
		}

		// get detail distribute map
		distributeMap, err := sj.cs.GetMap(util.TaskDistributeCollection, job)
		if err != nil {
			if util.IsNotFound(err) {
				// can't find on the remote, build an empty one
				distributeMap = map[string]interface{}{
					util.Md5Name:            []byte{},
					util.TaskDistributeName: make(map[string]interface{}),
				}
			} else {
				glog.Errorf("taskDistributeHandler: get TaskDistributeCollection map with job[%s] error[%v]",
					job, err)
				return err
			}
		}
		if _, ok := distributeMap[util.Md5Name]; !ok {
			glog.Errorf("taskDistributeHandler: md5 not exist in job[%s]", job)
			continue
		}

		// get local map
		if _, ok := sj.taskMap[job]; !ok {
			// add if not exist
			sj.taskMap[job] = make(map[string]interface{})
		}
		taskInsideMap := sj.taskMap[job].(map[string]interface{})
		// judge local md5 == remote md5
		if _, ok := taskInsideMap[util.Md5Name]; ok &&
			reflect.DeepEqual(taskInsideMap[util.Md5Name], distributeMap[util.Md5Name]) {
			continue
		}

		// do diff
		var isChange bool
		// local md5 value is copy from remote
		remoteList, ok := distributeMap[util.TaskDistributeName].(map[string]interface{})[sj.service].([]interface{})
		if !ok {
			// remote is empty
			remoteList = make([]interface{}, 0)
		}
		// local is map while remote is list
		addList, removeList := sj.diffTaskList(taskInsideMap, remoteList)
		sj.addTask(addList, job, jobInfoMap, taskInsideMap, &isChange)
		sj.removeTask(removeList, job, jobInfoMap, taskInsideMap, &isChange)

		if isChange {
			taskInsideMap[util.Md5Name] = distributeMap[util.Md5Name]
		}
	}

	return nil
}

// calculate md5sum
func (sj *SpecialJob) calTaskMd5(input map[string][]map[string]interface{}) []byte {
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	combineList := make([]string, 0, len(keys))
	for _, k := range keys {
		// calculate inner md5sum based on model.HostName
		md5Inner := util.CalInstanceListMd5(input[k], []string{model.HostName})
		combineList = append(combineList, fmt.Sprintf("%s->%v", k, md5Inner))
	}

	// calculate outer md5sum
	ans := strings.Join(combineList, "+")
	ansB := util.Md5([]byte(ans))
	return ansB[:]
}

// compare two lists and return the diff, addList is a map list and removeList is a *model.Instance list
func (sj *SpecialJob) diffTaskList(localMap map[string]interface{},
	remoteList []interface{}) (addList []map[string]interface{}, removeList []*model.Instance) {
	remoteMap := make(map[string]map[string]interface{})
	for _, ele := range remoteList {
		realEle := ele.(map[string]interface{})
		if v, ok := realEle[model.HostName].(string); !ok {
			glog.Warningf("SpecialJob diffTaskList 'host' not exist in map[%v]", realEle)
			continue
		} else {
			remoteMap[v] = realEle
		}
	}

	for key, val := range localMap {
		if _, ok := remoteMap[key]; !ok && key != util.Md5Name {
			removeList = append(removeList, val.(*model.Instance))
		}
	}
	for key, val := range remoteMap {
		if _, ok := localMap[key]; !ok && key != util.Md5Name {
			addList = append(addList, val)
		}
	}

	return addList, removeList
}

// add task
func (sj *SpecialJob) addTask(addList []map[string]interface{},
	job string, jobInfoMap map[string]interface{},
	taskInsideMap map[string]interface{}, isChange *bool) {
	for _, task := range addList {
		task = sj.taskMapComplement(task, jobInfoMap)
		ins := sj.convertMap2Instance(task)
		if ins == nil {
			glog.Errorf("convert map to instance error: job[%v] task[%s]", job, task)
			continue
		}

		// add into general job
		if err := sj.cm.CreateTask(job, ins); err != nil {
			glog.Errorf("add task error: job[%s] add task[%v] error[%v]", job, task, err)
		} else {
			taskInsideMap[ins.Addr] = ins
			*isChange = true
		}
	}
}

// remove task
func (sj *SpecialJob) removeTask(removeList []*model.Instance,
	job string, jobInfoMap map[string]interface{},
	taskInsideMap map[string]interface{}, isChange *bool) {
	for _, task := range removeList {
		if err := sj.cm.RemoveTask(task.Interval, job, task.Addr); err != nil {
			glog.Errorf("remove task error: job[%s] add task[%v] error[%v]", job, task, err)
		} else {
			delete(taskInsideMap, task.Addr)
			*isChange = true
		}
	}
}

func (sj *SpecialJob) convertMap2Instance(input map[string]interface{}) *model.Instance {
	ins := new(model.Instance)
	for key, val := range input {
		switch key {
		case model.PidName:
			if pid, err := util.ConvertInterface2Int(val); err != nil {
				return nil
			} else {
				ins.Pid = uint32(pid)
			}
		case model.HidName:
			if hid, err := util.ConvertInterface2Int(val); err != nil {
				return nil
			} else {
				ins.Hid = int32(hid)
			}
		case model.HostName:
			ins.Addr = val.(string)
		case model.UsernameName:
			ins.Username = val.(string)
		case model.PasswordName:
			ins.Password = val.(string)
		case model.DBTypeName:
			ins.DBType = val.(string)
		case model.Count:
			if count, err := util.ConvertInterface2Int(val); err == nil {
				ins.Count = count
			} else {
				return nil
			}
		case model.Interval:
			if interval, err := util.ConvertInterface2Int(val); err == nil {
				ins.Interval = interval
			} else {
				return nil
			}
		case model.Commands:
			var cmds = val.([]interface{})
			for _, it := range cmds {
				ins.Commands = append(ins.Commands, it.(string))
			}
		}
	}

	return ins
}

func (sj *SpecialJob) taskMapComplement(task, job map[string]interface{}) map[string]interface{} {
	for k, v := range job {
		if _, ok := task[k]; !ok {
			task[k] = v
		}
	}
	return task
}

// get interval
// func (sj *SpecialJob) getBaseInfo(job string) (int, int, error) {
// 	if mp, err := sj.cs.GetMap(util.MetaCollection, job); err != nil {
// 		return 0, 0, err
// 	} else {
// 		interval, ok := mp[util.IntervalName]
// 		if !ok {
// 			return 0, 0, fmt.Errorf("interval field not exists")
// 		}
//
// 		count, ok := mp[util.CountName]
// 		if !ok {
// 			return 0, 0, fmt.Errorf("count field not exists")
// 		}
//
// 		intervalInt, err := util.ConvertInterface2Int(interval)
// 		if err != nil || intervalInt <= 0 {
// 			return 0, 0, fmt.Errorf("job[%s] interval[%d] get error[%v]", job, intervalInt, err)
// 		}
// 		countInt, err := util.ConvertInterface2Int(count)
// 		if err != nil || countInt <= 0 {
// 			return 0, 0, fmt.Errorf("job[%s] count[%d] get error[%v]", job, countInt, err)
// 		}
//
// 		return intervalInt, countInt, nil
// 	}
// }
