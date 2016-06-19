package replication

import (
	"time"
	"log"
	"path"
	"sync"
	"bytes"
	"errors"
	"sync/atomic"
 	"strconv"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb"
)

const LOG_CACHE_SIZE = 40 << 20
const DATE_LAYOUT = "20060102"

var (
	ErrLogReadTimeout = errors.New("Log Position timeout")
)

type LevelReplicationLog struct {
	sync.Mutex
	Path string
	DB *leveldb.DB
	Counter uint64
}

func NewReplicationLog(basepath string) (*LevelReplicationLog) {
	options := &leveldb_opt.Options{
		BlockCacheCapacity : LOG_CACHE_SIZE,
		Filter:             leveldb_filter.NewBloomFilter(15),
	}
	path := path.Join(basepath, "replicatiion.log")
	db, err := leveldb.OpenFile(path, options)
	if err != nil {
		panic(err)
	}
	rplog := &LevelReplicationLog{Path : path, DB: db, Counter: 0}
	pos, err := rplog.GetCurrentPos()
	if err != nil {
		panic(err)
	}
	rplog.Counter = pos
	go purgeWorker(rplog)
	return rplog
}

func (rl *LevelReplicationLog) WriteLog(val []byte) (uint64, error) {
	rl.Lock()
	defer rl.Unlock()
	counter := []byte(strconv.FormatUint(rl.Counter +  1, 10))
	key := append([]byte("log:"), counter...)
	err := rl.DB.Put(key, val, nil)
	if err != nil {
		return 0 ,err
	}
	count := atomic.AddUint64(&rl.Counter, 1)
	return count, err
}

func (rl *LevelReplicationLog) GetLog(pos uint64) ([]byte, error) {	
	key := append([]byte("log:"), []byte(strconv.FormatUint(pos, 10))...)
	return rl.DB.Get(key, nil)
}

func (rl *LevelReplicationLog) GetLogFirstAvailable(pos uint64) (uint64, []byte, error) {
	timeout := 100
start:
	if timeout <= 0 {
		return 0, nil, ErrLogReadTimeout
	}
	timeout--
	key := append([]byte("log:"), []byte(strconv.FormatUint(pos, 10))...)
	v, err := rl.DB.Get(key, nil)
	if err == leveldb.ErrNotFound {
		pos++
		goto start
	} else if err != nil {
		return 0, nil, err
	}
	return pos, v, nil
}

func (rl *LevelReplicationLog) PurgeLogs(sPos uint64, ePos uint64) (error) {
	skey := append([]byte("log:"), []byte(strconv.FormatUint(ePos, 10))...)
	ekey := append([]byte("log:"), []byte(strconv.FormatUint(ePos, 10))...)
	batch := new(leveldb.Batch)
	iter := rl.DB.NewIterator(&util.Range{Start: skey, Limit: ekey}, nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	err := rl.DB.Write(batch, nil)
	iter.Release()
	return err
}

func (rl *LevelReplicationLog) GetCurrentPos() (uint64, error) {
	skey := append([]byte("log:"), []byte(strconv.FormatUint(0, 10))...)
	iter := rl.DB.NewIterator(&util.Range{Start: skey}, nil)
	var lastpos uint64 = 0
	for iter.Next() {
		if !bytes.HasPrefix(iter.Key(), []byte("log:")) {
			break
		}
		spltData := bytes.Split(iter.Key(), []byte(":"))
		if len(spltData) < 2 {
			continue
		}
		sp, err := strconv.ParseUint(string(spltData[1]), 10, 64)
		if err != nil {
			log.Println("Error while looking key, skipping", err)
			continue
		}
		if sp > lastpos {
			lastpos = sp
		}
	}
	return lastpos, nil
}

func (rl *LevelReplicationLog) SetDatePos(date []byte, pos uint64) (error) {
	key := append([]byte("date:"), date...)
	val := []byte(strconv.FormatUint(pos, 10))
	return rl.DB.Put(key, val, nil)
}

func (rl *LevelReplicationLog) GetDatePos(date []byte) (uint64, error) {
	key := append([]byte("date:"), date...)
	rawdata, err :=  rl.DB.Get(key, nil)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(rawdata), 10, 64)
}

func (rl *LevelReplicationLog) SetReaderPos(reader []byte, pos uint64) (error) {
	key := append([]byte("reader:"), reader...)
	val := []byte(strconv.FormatUint(pos, 10))
	return rl.DB.Put(key, val, nil)
}

func (rl *LevelReplicationLog) GetReaderPos(reader []byte) (uint64, error) {
	key := append([]byte("reader:"), reader...)
	rawdata, err :=  rl.DB.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(rawdata), 10, 64)
}

func (rl *LevelReplicationLog) DeleteReaderPos(reader []byte) (error) {
	key := append([]byte("reader:"), reader...)
	return rl.DB.Delete(key, nil)
}

func purgeWorker(rp *LevelReplicationLog) {
	commitUpdateTicker := time.NewTicker(300 * time.Second)
	purgeTicker := time.NewTicker(8600 * time.Second)
mainloop:
	for {
		select {
		case <-commitUpdateTicker.C:
			pos := rp.Counter
			err := rp.SetDatePos([]byte(time.Now().Format(DATE_LAYOUT)), pos)
			if err != nil {
				log.Println("Replication: Error while setting date pos", err)
			}
		case <-purgeTicker.C:
			pos, err := rp.GetDatePos([]byte(time.Now().Add(-(24 * 7) * time.Hour).Format(DATE_LAYOUT)))
			if err != nil {
				log.Println("Replication: Date Get error", err)
				continue mainloop
			}
			err = rp.PurgeLogs(0, pos)
			if err != nil {
				log.Println("Replication:  error while purging", err)
				continue mainloop
			}
			
		}
	}
}

func (rl *LevelReplicationLog) Close() {
	rl.DB.Close()
}

