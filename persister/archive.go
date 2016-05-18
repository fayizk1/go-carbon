package persister

import (
	"os"
	"log"
	"sync"
	"time"
	"sort"
	"path"
	"strconv"
	"errors"
	"github.com/fayizk1/go-carbon/points"
	"github.com/syndtr/goleveldb/leveldb"
)


type AtomicPoints struct {
	sync.Mutex
	points Points
}

type Archive struct {
	basepath string
	shards *Shards
	isopen bool
	sync.RWMutex
	mapd *LevelMap
}

type Shards struct {
	basepath string
	list map[string]*Shard
	sync.Mutex
}

func NewShards(basepath string) *Shards {
	return &Shards {
		basepath : basepath,
		list : make(map[string]*Shard),
	}
}

func (sds *Shards) GetShard(name string, create bool) *Shard {
	sds.Lock()
	defer sds.Unlock()
	if shard, ok := sds.list[name]; ok {
		return shard
	}
	if !create {
		fileInfo, err := os.Stat(path.Join(sds.basepath, name))
		if err != nil {
			log.Println("[archive] -", err)
			return nil
		} else if !fileInfo.IsDir() {
			return nil
		}
	}
	shard, err := Newshard(sds.basepath, name)
	if err != nil {
		panic(err)
	}
	sds.list[name] = shard
	return shard
}

func (sds *Shards) Close() {
	for _, s := range sds.list {
		s.Close()
	}
}

func NewArchive(basepath string, mapd *LevelMap) *Archive {
	err := os.MkdirAll(basepath, 0755)
	if err != nil {
		log.Println("Unable to open archive", err)
		panic(err.Error())
	}
	shards := NewShards(basepath)
	return &Archive {
		basepath : basepath,
		shards : shards,
		mapd : mapd,
		isopen : true, 
	}
}

func (ar *Archive) Store(key []byte, data *points.Points, sec ,exptime int64, aggMethod string) error {
	ar.RLock()
	defer ar.RUnlock()
	if !ar.isopen {
		return errors.New("Archive Closed")
	}
	finalData := make(map[int64]float64)
	for _, v := range data.Data {
		if v.Timestamp < exptime {
			//We drop if it is not relevant ts
			continue
		}
		tpoint := v.Timestamp - (v.Timestamp % sec)
		switch aggMethod {
		case "sum":
			if cv, ok := finalData[tpoint]; ok {
				finalData[tpoint] = cv + v.Value
			} else {
				finalData[tpoint] = v.Value
			}
		case "max" :
			if cv, ok := finalData[tpoint];ok {
				if v.Value > cv {
					finalData[tpoint] =  v.Value
				}
			} else {
				finalData[tpoint] = v.Value
			}
		case "min":
			if cv, ok := finalData[tpoint];ok {
				if v.Value < cv {
					finalData[tpoint] =  v.Value
				}
			} else {
				finalData[tpoint] = v.Value
			}
		case "average":
			if cv, ok := finalData[tpoint];ok {
				finalData[tpoint] =  (cv + v.Value) / 2
			} else {
				finalData[tpoint] = v.Value
			}
		}
	}
	storelist := make(map[string]*leveldb.Batch)
trans_loop:
	for t , v := range finalData {
		tn := time.Unix(t, 0).Format("20060102")
		_, ok := storelist[tn]
		if !ok {
			storelist[tn] =  new(leveldb.Batch)
		}
		kt := GenMetricKey(key, t)
		sh := ar.shards.GetShard(tn, true)
		d, err := sh.Get([]byte(kt))
		if err == nil {
			currentValue, err := strconv.ParseFloat(string(d), 64)
			if err != nil {
				log.Println("Error: Unable to convert stored value", err)
				continue
			}
			switch aggMethod {
			case "sum":
				v = currentValue + v
			case "max" :
				if v < currentValue {
					continue trans_loop
				}
			case "min":
				if v > currentValue {
					continue trans_loop
				}
			case "average":
				v =  (currentValue + v) / 2
			}
					
		}
		sv := strconv.FormatFloat(v, 'f', -1, 64)
		storelist[tn].Put(kt, []byte(sv))
	}
	for k, v := range storelist {
		sh := ar.shards.GetShard(k, true)
		err := sh.Write(v)
		if err != nil {
			log.Println("Error: Unable to write points into shard", k )
		}
	}
	return nil
}

func (ar *Archive) GetData(start, end int64, key []byte, sorting bool) Points {
	ar.RLock()
	defer ar.RUnlock()
	if !ar.isopen {
		return nil
	}
	amp := AtomicPoints{}
	start_key := GenMetricKey(key, start)
	end_key := GenMetricKey(key, end)
	ct := time.Unix(start, 0)
	et := time.Unix(end, 0)
	if ct.Day() > et.Day() {
		log.Println("Warning: query start time is higher than end time")
		return nil
	}
	var wg sync.WaitGroup
	for ; ct.Day() <= et.Day(); ct = ct.Add(24 * time.Hour) {
		shard := ar.shards.GetShard(ct.Format("20060102"), false)
		if shard == nil {
			log.Println("[persistor] Unable to locate shard", ct.Format("20060102"))
			continue
		}
		wg.Add(1)
		go func(sh *Shard) {
			defer wg.Done()
			pt := sh.RangeScan(start_key, end_key ,key)
			amp.Lock()
			defer amp.Unlock()
			amp.points = append(amp.points, pt...)
		}(shard)
	}
	wg.Wait()
	if sorting {
		sort.Sort(amp.points)
	}
	return amp.points
}

func (ar *Archive) Close() {
	ar.Lock()
	defer ar.Unlock()
	if !ar.isopen {
		return
	}
	ar.shards.Close()
	ar.isopen = false
}
