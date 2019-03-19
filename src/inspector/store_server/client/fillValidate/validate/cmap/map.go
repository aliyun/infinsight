package cmap

import(
    "fmt"
    "bytes"
)

// instance(name;count;step) -> key -> timestamp -> data
type CacheMap struct {
    cache map[string]map[string]map[uint32][]byte
    InstanceMap map[string]struct{}
}

func NewCacheMap() *CacheMap {
    return &CacheMap{
        cache: make(map[string]map[string]map[uint32][]byte),
        InstanceMap: make(map[string]struct{}),
    }
}

func (c *CacheMap) PutIns(name string) {
    c.InstanceMap[name] = struct{}{}
}

func (c *CacheMap) Put(name, key string, timestamp uint32, data []byte) error {
    if _, ok := c.cache[name]; !ok {
        c.cache[name] = make(map[string]map[uint32][]byte)
    }

    nameMap := c.cache[name]
    if _, ok := nameMap[key]; !ok {
        nameMap[key] = make(map[uint32][]byte)
    }

    timeMap := nameMap[key]
    if _, ok := timeMap[timestamp]; !ok {
        timeMap[timestamp] = data
    } else {
        return fmt.Errorf("duplicate insertion: name[%s], key[%s], timestamp[%u], data[%v]",
            name, key, timestamp, data)
    }
    return nil
}

func (c *CacheMap) Delete(name, key string, timestamp uint32, data []byte) error {
    if _, ok := c.cache[name]; !ok {
        return fmt.Errorf("can't find ins[%s] in cacheMap", name)
    }
    if _, ok := c.cache[name][key]; !ok {
        return fmt.Errorf("can't find ins[%s] key[%s] in cacheMap", name, key)
    }
    if _, ok := c.cache[name][key][timestamp]; !ok {
        return fmt.Errorf("can't find ins[%s] key[%s] timestamp[%d] in cacheMap", name, key, timestamp)
    }
    if bytes.Equal(data, c.cache[name][key][timestamp]) == false {
        return fmt.Errorf("find ins[%s] key[%s] timestamp[%d] in cacheMap but not equal", name, key, timestamp)
    }

    delete(c.cache[name][key], timestamp)
    if len(c.cache[name][key]) == 0 {
        delete(c.cache[name], key)
        if len(c.cache[name]) == 0 {
            delete(c.cache, name)
        }
    }
    return nil
}

func (c *CacheMap) Empty() bool {
    return len(c.cache) == 0
}
