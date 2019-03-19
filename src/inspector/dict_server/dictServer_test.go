package dictServer

import (
	"fmt"
	"testing"
	"time"
	"flag"
	"math/rand"
	"sync"

	"inspector/config"
	"inspector/util"

	"github.com/stretchr/testify/assert"
	"sort"
)

var (
	// test address
	address  string = "100.81.245.155:20111"
	username string = "admin"
	password string = "admin"
	db       string = "inspectorConfig"
)

func TestGet(t *testing.T) {
	var (
		err       error
		nr        int
		idxInt    int
		idxString string
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	c := &Conf{
		Address:    address,
		Username:   username,
		Password:   password,
		DB:         db,
		ServerType: "mongo",
	}

	// remove all item in section
	nr++
	fmt.Printf("TestGet case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	handler.DeleteSection(sectionName) // do not care about the return

	nr++
	fmt.Printf("TestGet case %d.\n", nr)
	s := NewDictServer(c, nil)
	idxString, err = s.GetValue("hello")
	assert.NotEqual(t, nil, err, "should be nil")

	time.Sleep(HandlerInterval * 4 * time.Millisecond)
	idxString, err = s.GetValue("hello")
	assert.Equal(t, nil, err, "should be nil")
	idxInt, err = util.RepString2Int(idxString)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 0, idxInt, "should be equal")

	// h2-h5000
	var testArr []string
	for i := 2; i <= 5000; i++ {
		testArr = append(testArr, fmt.Sprintf("h%d", i))
	}
	// testArr := []string{"h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9","h10"}
	for _, v := range testArr {
		idxString, err = s.GetValue(v)
		assert.NotEqual(t, nil, err, "should be nil")
	}
	time.Sleep(HandlerInterval * 4 * time.Millisecond)
	for _, v := range testArr {
		idxString, err = s.GetValue(v)
		assert.Equal(t, nil, err, "should be nil")
		v2, err := s.GetKey(idxString)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, v, v2, "should be nil")
	}

	// get key list
	nr++
	fmt.Printf("TestGet case %d.\n", nr)
	ret, err := s.GetKeyList()
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 5000, len(ret), "should be equal")
	// fmt.Println(ret)

	s.Close()
}

func TestGet2(t *testing.T) {
	var (
		err       error
		nr        int
		idxInt    int
		idxString string
	)
	flag.Set("stderrthreshold", "info")
	flag.Set("v", "2")

	c := &Conf{
		Address:    address,
		Username:   username,
		Password:   password,
		DB:         db,
		ServerType: "mongo",
	}

	// remove all item in section
	nr++
	fmt.Printf("TestGet2 case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	handler.DeleteSection(sectionName) // do not care about the return

	nr++
	fmt.Printf("TestGet2 case %d.\n", nr)
	s := NewDictServer(c, handler)
	idxString, err = s.GetValue("hello")
	assert.NotEqual(t, nil, err, "should be nil")

	time.Sleep(HandlerInterval * 4 * time.Millisecond)
	idxString, err = s.GetValue("hello")
	assert.Equal(t, nil, err, "should be nil")
	idxInt, err = util.RepString2Int(idxString)
	assert.Equal(t, nil, err, "should be nil")
	assert.Equal(t, 0, idxInt, "should be equal")

	// h2-h5000
	var testArr []string = []string{}
	for i := 2; i <= 5000; i++ {
		testArr = append(testArr, fmt.Sprintf("h%d", i))
	}
	// testArr := []string{"h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9","h10"}
	for _, v := range testArr {
		idxString, err = s.GetValue(v)
		assert.NotEqual(t, nil, err, "should be nil")
	}
	time.Sleep(HandlerInterval * 4 * time.Millisecond)
	for _, v := range testArr {
		idxString, err = s.GetValue(v)
		assert.Equal(t, nil, err, "should be nil")
		v2, err := s.GetKey(idxString)
		assert.Equal(t, nil, err, "should be nil")
		assert.Equal(t, v, v2, "should be nil")
	}

	s.Close()
}

//func TestDelete(t *testing.T) {
//	var (
//		err       error
//		nr        int
//		idxInt    int
//		idxString string
//	)
//
//	c := &Conf{
//		Address:    address,
//		Username:   username,
//		Password:   password,
//		DB:         db,
//		ServerType: "mongo",
//	}
//
//	// remove all item in section
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	factory := config.ConfigFactory{Name: "mongo"}
//	handler, err := factory.Create(address, username, password, db, -1)
//	handler.DeleteSection(sectionName) // do not care about the return
//
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	s := NewDictServer(c, handler)
//
//	// h1-h5
//	testArr := []string{"h1", "h2", "h3", "h4", "h5"}
//	for _, v := range testArr {
//		idxString, err = s.GetValue(v)
//		assert.NotEqual(t, nil, err, "should be nil")
//	}
//	time.Sleep(HandlerInterval * 2 * time.Millisecond)
//
//	for _, v := range testArr {
//		idxString, err = s.GetValue(v)
//		assert.Equal(t, nil, err, "should be nil")
//		v2, err := s.GetKey(idxString)
//		assert.Equal(t, nil, err, "should be nil")
//		assert.Equal(t, v, v2, "should be nil")
//	}
//
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	idxString2, err := s.GetValue("h2")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt2, err := util.RepString2Int(idxString2)
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h2")
//	assert.Equal(t, nil, err, "should be nil")
//
//	idxString5, err := s.GetValue("h5")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt5, err := util.RepString2Int(idxString5)
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h5")
//	assert.Equal(t, nil, err, "should be nil")
//
//	// fmt.Printf("idx2: %v idx5: %v\n", idx2, idx5)
//	time.Sleep(HandlerInterval * 4 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	_, _ = s.GetValue("h6")
//
//	time.Sleep(HandlerInterval * 4 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	idxString, err = s.GetValue("h6")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt, err = util.RepString2Int(idxString)
//	assert.Equal(t, nil, err, "should be nil")
//	assert.Equal(t, int(math.Min(float64(idxInt2), float64(idxInt5))), idxInt, "should be nil")
//
//	time.Sleep(HandlerInterval * 2 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	_, _ = s.GetValue("h7")
//	time.Sleep(HandlerInterval * 2 * time.Millisecond)
//	idxString, err = s.GetValue("h7")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt, err = util.RepString2Int(idxString)
//	assert.Equal(t, nil, err, "should be nil")
//	assert.Equal(t, int(math.Max(float64(idxInt2), float64(idxInt5))), idxInt, "should be nil")
//
//	// get h2 again
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	_, _ = s.GetValue("h2")
//	time.Sleep(HandlerInterval * 4 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	idxString, err = s.GetValue("h2")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt, err = util.RepString2Int(idxString)
//	assert.Equal(t, nil, err, "should be nil")
//	assert.Equal(t, 5, idxInt, "should be nil")
//
//	// get h5 again
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	_, _ = s.GetValue("h5")
//	time.Sleep(HandlerInterval * 4 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	idxString, err = s.GetValue("h5")
//	assert.Equal(t, nil, err, "should be nil")
//	assert.Equal(t, util.RepInt2String(6), idxString, "should be nil")
//
//	// delete h5 again
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	err = s.Delete("h5")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h5")
//	assert.NotEqual(t, nil, err, "should not be nil")
//	idxString, err = s.GetValue("h5")
//	assert.NotEqual(t, nil, err, "should be nil")
//
//	time.Sleep(HandlerInterval * 4 * time.Millisecond)
//	// get h5 again
//	idxString, err = s.GetValue("h5")
//	if err != nil {
//		time.Sleep(HandlerInterval * 2 * time.Millisecond)
//		idxString, err = s.GetValue("h5")
//	}
//	assert.Equal(t, nil, err, "should be nil")
//	if idxString != util.RepInt2String(6) && idxString != util.RepInt2String(6) {
//		assert.Equal(t, "", idxString, "idx not equal to 6 or 7")
//	}
//
//	// delete h1-h7
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	err = s.Delete("h1")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h2")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h3")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h4")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h5")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h6")
//	assert.Equal(t, nil, err, "should be nil")
//	err = s.Delete("h7")
//	assert.Equal(t, nil, err, "should be nil")
//
//	// need sleep a while, request may lose otherwise
//	time.Sleep(HandlerInterval * 2 * time.Millisecond)
//	nr++
//	fmt.Printf("TestDelete case %d.\n", nr)
//	idxString, err = s.GetValue("h6")
//	assert.NotEqual(t, nil, err, "should be nil")
//	idxString, err = s.GetValue("h7")
//	assert.NotEqual(t, nil, err, "should be nil")
//	idxString, err = s.GetValue("h8")
//	assert.NotEqual(t, nil, err, "should be nil")
//	time.Sleep(HandlerInterval * 2 * time.Millisecond)
//	idxString6, err := s.GetValue("h6")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt6, err := util.RepString2Int(idxString6)
//	assert.Equal(t, nil, err, "should be nil")
//	idxString7, err := s.GetValue("h7")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt7, err := util.RepString2Int(idxString7)
//	assert.Equal(t, nil, err, "should be nil")
//	idxString8, err := s.GetValue("h8")
//	assert.Equal(t, nil, err, "should be nil")
//	idxInt8, err := util.RepString2Int(idxString8)
//	assert.Equal(t, nil, err, "should be nil")
//	mi := math.Min(math.Min(float64(idxInt6), float64(idxInt7)), float64(idxInt8))
//	mx := math.Max(math.Max(float64(idxInt6), float64(idxInt7)), float64(idxInt8))
//	if mi+2 != mx || mx >= 8 || mi < 0 {
//		assert.Equal(t, 1, 2, fmt.Sprintf("idx6:%d idx7:%d idx8:%d",
//			idxInt6, idxInt7, idxInt8))
//	}
//
//	s.Close()
//}

//func TestConcurrency1(t *testing.T) {
//	var wait sync.WaitGroup
//	var arr [20][30]int
//	testArr := []string{"h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12", "h13", "h14", "h15", "h16", "h17", "h18", "h19", "h20", "h21", "h22", "h23", "h24", "h25", "h26", "h27", "h28", "h29", "h30"}
//	var failTime int32
//	call := func(i int, res *[30]int, testArr []string) {
//		var (
//			err       error
//			idxString string
//			idxInt    int
//		)
//
//		c := &Conf{
//			Address:    address,
//			Username:   username,
//			Password:   password,
//			DB:         db,
//			ServerType: "mongo",
//		}
//
//		// remove all item in section
//		factory := config.ConfigFactory{Name: "mongo"}
//		handler, err := factory.Create(address, username, password, db, 1000)
//		handler.DeleteSection(sectionName) // do not care about the return
//
//		s := NewDictServer(c, handler)
//
//		for _, v := range testArr {
//			idxString, err = s.GetValue(v)
//			assert.NotEqual(t, nil, err, "should be nil")
//		}
//
//		time.Sleep(HandlerInterval * 5 * time.Millisecond)
//		for i, v := range testArr {
//			idxString, err = s.GetValue(v)
//			assert.Equal(t, nil, err, "should be nil")
//			idxInt, err = util.RepString2Int(idxString)
//			assert.Equal(t, nil, err, "should be nil")
//			res[i] = idxInt
//		}
//
//		time.Sleep(HandlerInterval * 5 * time.Millisecond)
//		// try to delete one
//		item := fmt.Sprintf("h%d", i)
//		if err := s.Delete(item); err != nil { // at least one can success
//			atomic.AddInt32(&failTime, 1)
//		}
//
//		s.Close()
//		wait.Done()
//	}
//
//	for i := 0; i < 20; i++ {
//		wait.Add(1)
//		go call(i+1, &arr[i], testArr)
//	}
//	wait.Wait()
//
//	nr := 1
//	fmt.Printf("TestConcurrency1 case %d.\n", nr)
//	orSum := 0
//	for i := 0; i < 30; i++ {
//		orSum |= 1 << uint(arr[0][i])
//	}
//	assert.Equal(t, (1<<30)-1, orSum, "should be nil")
//
//	for i := 1; i < 20; i++ {
//		assert.Equal(t, arr[0], arr[i], "should be nil")
//	}
//
//	nr++
//	fmt.Printf("TestConcurrency1 case %d.\n", nr)
//	assert.Equal(t, true, failTime <= 19, "should be equal")
//}

func TestConcurrency2(t *testing.T) {
	var nr int

	{
		nr++
		fmt.Printf("TestConcurrency2 case %d.\n", nr)
		factory := config.ConfigFactory{Name: "mongo"}
		handler, err := factory.Create(address, username, password, db, -1)
		assert.Equal(t, nil, err, "should be nil")
		handler.DeleteSection(sectionName) // do not care about the return
	}

	{
		nr++
		fmt.Printf("TestConcurrency2 case %d.\n", nr)

		var wait sync.WaitGroup
		keyNr := 5000
		testArr := make([]string, 0, keyNr)
		for i := 0; i < keyNr; i++ {
			testArr = append(testArr, fmt.Sprintf("concurrency-%d", i))
		}

		// global map
		global := new(sync.Map)

		call := func(testArr []string, nr, tot int) {
			fmt.Printf("function called: nr[%d] tot[%d]\n", nr, tot)

			r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
			time.Sleep(time.Duration(r.Intn(10000)) * time.Millisecond) // random sleep [0, 3s]
			fmt.Printf("call[%d] wakes up\n", nr)

			c := &Conf{
				Address:    address,
				Username:   username,
				Password:   password,
				DB:         db,
				ServerType: "mongo",
			}
			s := NewDictServer(c, nil)
			assert.NotEqual(t, nil, s, "should be nil")

			needNext := true
			cnt := 0
			for ; needNext; {
				time.Sleep(1 * time.Second)
				cnt++
				fmt.Printf("call[%d] try [%d]\n", nr, cnt)
				needNext = false
				for i, v := range testArr {
					if i % tot == nr || i % tot == nr + 1 {
						fmt.Printf("call[%d] use key[%s]\n", nr, v)
						idxString, err := s.GetValue(v)
						if err == nil {
							if gv, ok := global.Load(v); ok {
								assert.Equal(t, true, ok, "should be equal")
								assert.Equal(t, idxString, gv.(string), "should be equal")
							} else {
								global.Store(v, idxString) // store if not exist
							}
						} else {
							needNext = true
						}
					}
				}
			}

			fmt.Printf("call[%d] first query loop end\n", nr)
			wait.Done()
		}

		routineNr := 40
		for i := 0; i < routineNr; i++ {
			wait.Add(1)
			go call(testArr, i, routineNr)
		}

		wait.Wait()
		fmt.Println("all routine exit")

		// check global map
		valueList := make([]int, 0)
		global.Range(func(key, val interface{}) bool {
			intVal, err := util.RepString2Int(val.(string))
			assert.Equal(t, nil, err, "should be equal")
			valueList = append(valueList, intVal)
			return true
		})

		assert.Equal(t, keyNr, len(valueList), "should be equal")
		sort.Ints(valueList)
		for i := 0; i < len(valueList); i++ {
			assert.Equal(t, i, valueList[i], "should be equal")
		}
	}
}

func TestGetValueOnly(t *testing.T) {
	var (
		nr  int
		err error
	)

	c := &Conf{
		Address:    address,
		Username:   username,
		Password:   password,
		DB:         db,
		ServerType: "mongo",
	}

	// remove all item in section
	nr++
	fmt.Printf("TestGetValueOnly case %d.\n", nr)
	factory := config.ConfigFactory{Name: "mongo"}
	handler, err := factory.Create(address, username, password, db, -1)
	handler.DeleteSection(sectionName) // do not care about the return

	nr++
	fmt.Printf("TestGetValueOnly case %d.\n", nr)
	s := NewDictServer(c, handler)

	for i := 0; i < 5; i++ {
		input := fmt.Sprintf("h%d", i)
		_, err = s.GetValueOnly(input)
		assert.NotEqual(t, nil, err, "shouldn't be nil")
	}

	nr++
	fmt.Printf("TestGetValueOnly case %d.\n", nr)

	time.Sleep((HandlerInterval + 2000) * time.Millisecond)

	for i := 0; i < 10; i++ {
		input := fmt.Sprintf("h%d", i)
		_, err = s.GetValueOnly(input)
		assert.NotEqual(t, nil, err, "shouldn't be nil")
	}

	{
		nr++
		fmt.Printf("TestGetValueOnly case %d.\n", nr)

		// get h1-h5
		testArr := []string{"h1", "h2", "h3", "h4", "h5"}
		for _, v := range testArr {
			_, err = s.GetValue(v)
			assert.NotEqual(t, nil, err, "should be nil")
		}
		time.Sleep(HandlerInterval * 2 * time.Millisecond)

		nr++
		fmt.Printf("TestGetValueOnly case %d.\n", nr)
		for _, v := range testArr {
			_, err = s.GetValue(v)
			assert.Equal(t, nil, err, "should be nil")
		}
	}

	{
		// create new dict server
		s2 := NewDictServer(c, handler)

		var sum = 0
		testArr := []string{"h1", "h2", "h3", "h4", "h5"}
		for _, v := range testArr {
			ret, err := s2.GetValueOnly(v)
			assert.Equal(t, nil, err, "should be nil")
			retInt, err := util.RepString2Int(ret)
			sum += retInt
		}
		assert.Equal(t, 10, sum, "should be nil")

		_, err := s2.GetValueOnly("h6")
		assert.NotEqual(t, nil, err, "should be nil")
	}
}
