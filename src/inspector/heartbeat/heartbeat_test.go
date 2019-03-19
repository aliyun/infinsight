package heartbeat

import (
	"fmt"
	"testing"
	"time"
	"sync"

	"inspector/config"

	"github.com/stretchr/testify/assert"
	"flag"
)

var (
	// test address
	// address  string = "10.101.72.137:3001"
	address  string = "10.101.72.137:20001"
	// address  string = "100.81.245.155:20111"
	username string = "admin"
	password string = "admin"
	db       string = "inspectorConfig"
)

func TestStart(t *testing.T) {
	return
	var (
		err error
		nr  int
	)
	c := &Conf{
		Module:   ModuleApi,
		Service:  "10_1_1_1:123",
		Interval: 10,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestStart case %d.\n", nr)
	h := NewHeartbeat(c)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestStart case %d.\n", nr)
	err = h.Start()
	assert.Equal(t, nil, err, "should be nil")

	h.Close()
}

func TestGetModulesAndServices(t *testing.T) {
	var (
		err           error
		nr            int
		// retAllModules map[ModuleType]map[string]bool
		retAllModules map[ModuleType][]*NodeStatus
		//retPartialModules  map[ModuleType][]string
		//retAllServices     map[string]bool
		retPartialServices []*NodeStatus
	)

	// remove all item in section
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	handler.DeleteSection(SectionName) // do not care about the return

	c0 := &Conf{
		Module:   ModuleApi,
		Service:  "10.1.1.1:123",
		Interval: 5,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h := NewHeartbeat(c0)
	assert.Equal(t, (*Heartbeat)(nil), h, "should be nil")

	c := &Conf{
		Module:   ModuleApi,
		Service:  "10_1_1_1:123",
		Interval: 5,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h = NewHeartbeat(c)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	err = h.Start()
	assert.Equal(t, nil, err, "should be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retAllModules, err = h.GetModules(ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 3, len(retAllModules), "should be equal")
	assert.Equal(t, 1, getStatus(retAllModules[ModuleApi], c.Service), "should be equal")
	assert.Equal(t, 1, len(retAllModules[ModuleApi]), "should be equal")
	assert.Equal(t, 0, len(retAllModules[ModuleStore]), "should be equal")
	assert.Equal(t, 0, len(retAllModules[ModuleCollector]), "should be equal")

	// add c2
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	c2 := &Conf{
		Module:   ModuleApi,
		Service:  "10_1_1_2:123",
		Interval: 5,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h2 := NewHeartbeat(c2)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	err = h2.Start()
	assert.Equal(t, nil, err, "should be nil")

	// add c3
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	c3 := &Conf{
		Module:   ModuleCollector,
		Service:  "10_1_1_2:124",
		Interval: 3,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h3 := NewHeartbeat(c3)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	err = h3.Start()
	assert.Equal(t, nil, err, "should be nil")

	// add c4
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	c4 := &Conf{
		Module:   ModuleStore,
		Service:  "10_1_1_2:125",
		Interval: 2,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h4 := NewHeartbeat(c4)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	err = h4.Start()
	assert.Equal(t, nil, err, "should be nil")

	// get
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retAllModules, err = h.GetModules(ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 3, len(retAllModules), "should be equal")
	assert.Equal(t, 1, getStatus(retAllModules[ModuleApi], c.Service), "should be equal")
	assert.Equal(t, 1, getStatus(retAllModules[ModuleApi], c2.Service), "should be equal")
	assert.Equal(t, 1, getStatus(retAllModules[ModuleCollector], c3.Service), "should be equal")
	assert.Equal(t, 1, getStatus(retAllModules[ModuleStore], c4.Service), "should be equal")
	assert.Equal(t, 2, len(retAllModules[c.Module]), "should be equal")
	assert.Equal(t, 1, len(retAllModules[ModuleStore]), "should be equal")
	assert.Equal(t, 1, len(retAllModules[ModuleCollector]), "should be equal")

	// add c4 again
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h4_2 := NewHeartbeat(c4)
	assert.NotEqual(t, nil, h, "shouldn't be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	err = h4_2.Start()
	assert.NotEqual(t, nil, err, "shouldn't be nil")

	// get dead
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retPartialServices, err = h.GetServices(ModuleStore, ServiceDead)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 0, len(retPartialServices), "should be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h4.Close()
	time.Sleep(time.Duration(c4.Interval*3) * time.Second)

	// get dead
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retPartialServices, err = h.GetServices(ModuleStore, ServiceDead)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 0, len(retPartialServices), "should be nil")

	time.Sleep(time.Duration(c4.Interval*3) * time.Second)
	// get dead again
	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retPartialServices, err = h.GetServices(ModuleStore, ServiceDead)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 1, len(retPartialServices), "should be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	h4_3 := NewHeartbeat(c4)
	err = h4_3.Start()
	assert.Equal(t, nil, err, "should be nil")

	time.Sleep(time.Duration(c4.Interval) * time.Second)
	retPartialServices, err = h.GetServices(ModuleStore, ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 1, len(retPartialServices), "should be nil")

	nr++
	fmt.Printf("TestGetModulesAndServices case %d.\n", nr)
	retPartialServices, err = h.GetServices(ModuleStore, ServiceDead)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 0, len(retPartialServices), "should be nil")

	h.Close()
	h2.Close()
	h3.Close()
	h4_2.Close()
	h4_3.Close()
}

func TestRegister2(t *testing.T) {
	var (
		err           error
		nr            int
	)

	// remove all item in section
	nr++
	fmt.Printf("TestRegister2 case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	assert.Equal(t, nil, err, "should be nil")
	handler.DeleteSection(SectionName) // do not care about the return

	c0 := &Conf{
		Module:   ModuleCollector,
		Service:  "10_1_1_1:123",
		Interval: 2,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestRegister2 case %d.\n", nr)
	h := NewHeartbeat(c0)
	assert.NotEqual(t, (*Heartbeat)(nil), h, "should be nil")
	err = h.Start()
	assert.Equal(t, nil, err, "should be nil")
	nodes, err := h.GetServices(ModuleCollector, ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 1, len(nodes), "should be nil")
	assert.Equal(t, int32(0), nodes[0].Gid, "should be nil")
	gid, err := h.GetServiceCount(ModuleCollector)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 1, gid, "should be nil")

	c := &Conf{
		Module:   ModuleCollector,
		Service:  "10_1_1_1:124",
		Interval: 1,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestRegister2 case %d.\n", nr)
	h1 := NewHeartbeat(c)
	assert.NotEqual(t, nil, h, "shouldn't be nil")
	err = h1.Start()
	assert.Equal(t, nil, err, "should be nil")
	nodes, err = h.GetServices(ModuleCollector, ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 2, len(nodes), "should be nil")
	assert.Equal(t, int32(0), nodes[0].Gid, "should be nil")
	assert.Equal(t, int32(1), nodes[1].Gid, "should be nil")
	gid, err = h.GetServiceCount(ModuleCollector)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 2, gid, "should be nil")

	c2 := &Conf{
		Module:   ModuleCollector,
		Service:  "10_1_1_1:125",
		Interval: 1,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestRegister2 case %d.\n", nr)
	h2 := NewHeartbeat(c2)
	assert.NotEqual(t, nil, h, "shouldn't be nil")
	err = h2.Start()
	assert.Equal(t, nil, err, "should be nil")
	nodes, err = h.GetServices(ModuleCollector, ServiceBoth)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 3, len(nodes), "should be nil")
	assert.Equal(t, int32(0), nodes[0].Gid, "should be nil")
	assert.Equal(t, int32(1), nodes[1].Gid, "should be nil")
	assert.Equal(t, int32(2), nodes[2].Gid, "should be nil")
	gid, err = h.GetServiceCount(ModuleCollector)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 3, gid, "should be nil")

	h1.Close()
	h2.Close()
	h.Close()

	time.Sleep(10 * time.Second)
	// restart h1 again
	h1 = NewHeartbeat(c)
	assert.NotEqual(t, nil, h, "shouldn't be nil")
	err = h1.Start()
	assert.Equal(t, nil, err, "should be nil")
	gid, err = h.GetServiceCount(ModuleCollector)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 3, gid, "should be nil")
	h1.Close()
}


// concurrency register
func TestConcurrency1(t *testing.T) {
	var nr int
	var global *Heartbeat

	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	{
		// remove all item in section
		nr++
		fmt.Printf("TestConcurrency1 case %d.\n", nr)
		factory := config.ConfigFactory{Name: "mongo"}
		handler, err := factory.Create(address, username, password, db, -1)
		handler.DeleteSection(SectionName) // do not care about the return
		assert.Equal(t, nil, err, "should be nil")
	}

	{
		nr++
		fmt.Printf("TestConcurrency1 case %d.\n", nr)

		c := &Conf{
			Module:   ModuleApi,
			Service:  fmt.Sprintf("200_1_2_3:%d", 1999),
			Interval: 3,
			Address:  address,
			Username: username,
			Password: password,
			DB:       db,
		}

		global = NewHeartbeat(c)
		assert.Equal(t, false, global == nil, "should be nil")
		err := global.Start()
		assert.Equal(t, nil, err, "should be nil")
	}

	cnt := 20
	{
		nr++
		fmt.Printf("TestConcurrency1 case %d.\n", nr)

		var wait sync.WaitGroup
		for i := 0; i < cnt; i++ {
			wait.Add(1)
			go func(idx int) {
				c := &Conf{
					Module:   ModuleApi,
					Service:  fmt.Sprintf("200_1_2_3:%d", idx + 1000),
					Interval: 1,
					Address:  address,
					Username: username,
					Password: password,
					DB:       db,
				}
				h := NewHeartbeat(c)
				assert.Equal(t, false, h == nil, "should be nil")
				err := h.Start()
				assert.Equal(t, nil, err, "should be nil")
				h.Close()
				wait.Done()
			}(i)
		}

		wait.Wait()
		ret, err := global.GetServices(ModuleApi, ServiceBoth)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, cnt + 1, len(ret), "should be nil")

		for i, ele := range ret {
			// assert.Equal(t, ele.Alive, true, "should be nil")
			assert.Equal(t, ele.Gid, int32(i), "should be nil")
		}
	}

	time.Sleep(8 * time.Second)
	{
		nr++
		fmt.Printf("TestConcurrency1 case %d.\n", nr)
		// don't clear previous 20 items, try to update

		var wait sync.WaitGroup
		for i := 0; i < cnt; i++ {
			wait.Add(1)
			go func(idx int) {
				c := &Conf{
					Module:   ModuleApi,
					Service:  fmt.Sprintf("200_1_2_4:%d", idx + 1000),
					Interval: 1,
					Address:  address,
					Username: username,
					Password: password,
					DB:       db,
				}
				nr++
				h := NewHeartbeat(c)
				assert.Equal(t, false, h == nil, "should be nil")
				err := h.Start()
				assert.Equal(t, nil, err, "should be nil")
				h.Close()
				wait.Done()
			}(i)
		}

		wait.Wait()
		ret, err := global.GetServices(ModuleApi, ServiceBoth)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, cnt * 2 + 1, len(ret), "should be nil")

		for i, ele := range ret {
			// assert.Equal(t, ele.Alive, true, "should be nil")
			assert.Equal(t, ele.Gid, int32(i), "should be nil")
		}
	}

	global.Close()
}

func TestSort(t *testing.T) {
	var (
		nr int
		h, h1, h2 *Heartbeat
	)

	{
		// remove all item in section
		nr++
		fmt.Printf("TestSort case %d.\n", nr)
		factory := config.ConfigFactory{Name: "mongo"}
		handler, err := factory.Create(address, username, password, db, -1)
		handler.DeleteSection(SectionName) // do not care about the return
		assert.Equal(t, nil, err, "should be nil")
	}

	{
		nr++
		fmt.Printf("TestSort case %d.\n", nr)

		c0 := &Conf{
			Module:   ModuleApi,
			Service:  "10_1_1_1:123",
			Interval: 5,
			Address:  address,
			Username: username,
			Password: password,
			DB:       db,
		}
		h = NewHeartbeat(c0)
		h.Start()
	}

	{
		nr++
		fmt.Printf("TestSort case %d.\n", nr)

		c := &Conf{
			Module:   ModuleApi,
			Service:  "10_1_1_1:124",
			Interval: 5,
			Address:  address,
			Username: username,
			Password: password,
			DB:       db,
		}
		h1 = NewHeartbeat(c)
		h1.Start()

		ret, err := h.GetServices(ModuleApi, ServiceBoth)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, "10_1_1_1:123", ret[0].Name, "should be nil")
		assert.Equal(t, "10_1_1_1:124", ret[1].Name, "should be nil")
	}

	{
		nr++
		fmt.Printf("TestSort case %d.\n", nr)

		c := &Conf{
			Module:   ModuleApi,
			Service:  "9_1_1_1:124",
			Interval: 1,
			Address:  address,
			Username: username,
			Password: password,
			DB:       db,
		}
		h2 = NewHeartbeat(c)
		h2.Start()

		ret, err := h.GetServices(ModuleApi, ServiceBoth)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, "10_1_1_1:123", ret[0].Name, "should be nil")
		assert.Equal(t, "10_1_1_1:124", ret[1].Name, "should be nil")
		assert.Equal(t, "9_1_1_1:124", ret[2].Name, "should be nil")
		assert.Equal(t, true, ret[0].Alive, "should be nil")
		assert.Equal(t, true, ret[1].Alive, "should be nil")
		assert.Equal(t, true, ret[2].Alive, "should be nil")

		h2.Close()
		time.Sleep(time.Duration(c.Interval * 6) * time.Second)

		ret, err = h.GetServices(ModuleApi, ServiceBoth)
		assert.Equal(t, "10_1_1_1:123", ret[0].Name, "should be nil")
		assert.Equal(t, "10_1_1_1:124", ret[1].Name, "should be nil")
		assert.Equal(t, "9_1_1_1:124", ret[2].Name, "should be nil")
		assert.Equal(t, true, ret[0].Alive, "should be nil")
		assert.Equal(t, true, ret[1].Alive, "should be nil")
		assert.Equal(t, false, ret[2].Alive, "should be nil")
	}

	h1.Close()
	h.Close()
}

func TestWatcher(t *testing.T) {
	var (
		err error
		nr  int
		//retAllModules      map[ModuleType]map[string]bool
		//retPartialModules  map[ModuleType][]string
		//retAllServices     map[string]bool
		//retPartialServices []string
	)
	// remove all item in section
	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	handler.DeleteSection(SectionName) // do not care about the return

	c := &Conf{
		Module:   ModuleApi,
		Service:  "10_1_1_1:123",
		Interval: 5,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	h := NewHeartbeat(c)
	assert.NotEqual(t, nil, h, "shouldn't be nil")
	err = h.Start()
	assert.Equal(t, nil, err, "should be nil")

	var eventCollector2Connect bool
	watcher1 := &Watcher{
		Event: WatcherConnect,
		Handler: func(event WatchEvent) error {
			eventCollector2Connect = true
			return nil
		},
	}
	err = h.RegisterGlobalWatcher(ModuleCollector, "20_2_2_2:2", watcher1)
	assert.Equal(t, nil, err, "shouldn't be nil")

	c2 := &Conf{
		Module:   ModuleCollector,
		Service:  "20_2_2_2:2",
		Interval: 3,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	h2 := NewHeartbeat(c2)
	assert.NotEqual(t, nil, h, "shouldn't be nil")
	err = h2.Start()
	assert.Equal(t, nil, err, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	time.Sleep(time.Duration(c.Interval+1) * time.Second)
	assert.Equal(t, true, eventCollector2Connect, "should be nil")

	eventCollector2Connect = false
	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	time.Sleep(time.Duration(c.Interval+1) * time.Second)
	assert.Equal(t, false, eventCollector2Connect, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	var eventCollector2DisConnect bool
	watcher2 := &Watcher{
		Event: WatcherDisconnect,
		Handler: func(event WatchEvent) error {
			eventCollector2DisConnect = true
			return nil
		},
	}
	err = h.RegisterGlobalWatcher(ModuleCollector, "20_2_2_2:2", watcher2)
	h2.Close()
	time.Sleep(time.Duration(c.Interval*6) * time.Second)
	assert.Equal(t, true, eventCollector2DisConnect, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	var eventCollector3All bool
	watcher3 := &Watcher{
		Event: WatcherAll,
		Handler: func(event WatchEvent) error {
			eventCollector3All = true
			fmt.Printf("event[%v] happen\n", event)
			return nil
		},
	}
	err = h.RegisterGlobalWatcher(ModuleCollector, ServiceAll, watcher3)

	c3_1 := &Conf{
		Module:   ModuleCollector,
		Service:  "30_3_3_3:2",
		Interval: 3,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	c3_2 := &Conf{
		Module:   ModuleApi,
		Service:  "30_3_3_3:2",
		Interval: 3,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	h3_1 := NewHeartbeat(c3_1)
	assert.NotEqual(t, nil, h3_1, "shouldn't be nil")
	err = h3_1.Start()
	assert.Equal(t, nil, err, "should be nil")
	time.Sleep(time.Duration(c.Interval) * time.Second)
	assert.Equal(t, true, eventCollector3All, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	eventCollector3All = false
	h3_2 := NewHeartbeat(c3_2)
	assert.NotEqual(t, nil, h3_2, "shouldn't be nil")
	err = h3_2.Start()
	assert.Equal(t, nil, err, "should be nil")
	time.Sleep(time.Duration(c.Interval) * time.Second)
	assert.Equal(t, false, eventCollector3All, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	h3_1.Close()
	time.Sleep(time.Duration(c.Interval*6) * time.Second)
	assert.Equal(t, true, eventCollector3All, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	var eventAll bool
	watcher4 := &Watcher{
		Event: WatcherAll,
		Handler: func(event WatchEvent) error {
			eventAll = true
			return nil
		},
	}
	err = h.RegisterGlobalWatcher(ModuleAll, ServiceAll, watcher4)
	h3_2.Close()
	time.Sleep(time.Duration(c.Interval*6) * time.Second)
	assert.Equal(t, true, eventCollector3All, "should be nil")

	nr++
	fmt.Printf("TestWatcher case %d.\n", nr)
	c4 := &Conf{
		Module:   ModuleStore,
		Service:  "30_3_3_3:1000",
		Interval: 2,
		Address:  address,
		Username: username,
		Password: password,
		DB:       db,
	}
	h4 := NewHeartbeat(c4)
	assert.NotEqual(t, nil, h4, "shouldn't be nil")
	err = h4.Start()
	assert.Equal(t, nil, err, "should be nil")
	time.Sleep(time.Duration(c.Interval) * time.Second)
	assert.Equal(t, true, eventAll, "should be nil")
}

// 0: not alive, 1: alive, 2: not exist
func getStatus(input []*NodeStatus, host string) int {
	for _, node := range input {
		if node.Name == host {
			if node.Alive {
				return 1
			}
			return 0
		}
	}
	return 2
}
