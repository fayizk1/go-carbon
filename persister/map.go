package persister

import (
	"path"
	"errors"
	"strconv"
	"sync"
	"time"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/Sirupsen/logrus"
)

const MAP_CACHE_SIZE = 40 << 20

var (
	ErrCreateRateLimit = errors.New("High create request")
)

type throttleMetaData struct {
	sync.Mutex
	Expiry int64
	SamplePatten []string
	Rate int
	RateLimit int
	RateLimitPeriod int
	SampleCount int
	Enabled bool
	DisabledWrite bool
	DisabledTime int64
}

type LevelMap struct {
	ThrottleMetaData *throttleMetaData
	Path string
	DB *leveldb.DB
}

func NewMap(basepath string, rateLimit, rateLimitPeriod, sampleCount int) (*LevelMap) {
	options := &leveldb_opt.Options{
		BlockCacheCapacity : MAP_CACHE_SIZE,
		Filter:             leveldb_filter.NewBloomFilter(15),
	}
	path := path.Join(basepath, "carbon.map")
	db, err := leveldb.OpenFile(path, options)
	if err != nil {
		panic(err)
	}
	tmd := &throttleMetaData{Expiry:0, SamplePatten : nil, RateLimit: rateLimit, RateLimitPeriod: rateLimitPeriod, SampleCount : sampleCount, Enabled : true, DisabledWrite: false, DisabledTime: 0}
	return &LevelMap{Path : path, DB: db, ThrottleMetaData: tmd,}
}

func (mp *LevelMap) GetShortKey(key string, iswrite bool) ([]byte, error) {
	okey := key
	key = "key:" + key
	keydata, err := mp.DB.Get([]byte(key), nil)
	if !iswrite {
		return keydata, err
	}
	if err == leveldb.ErrNotFound {
		mp.ThrottleMetaData.Lock()
		defer mp.ThrottleMetaData.Unlock()
		if mp.ThrottleMetaData.DisabledWrite {
			if time.Now().Unix() > mp.ThrottleMetaData.DisabledTime {
				mp.ThrottleMetaData.DisabledWrite = false
			} else {
				return nil, ErrCreateRateLimit
			}
		}
		if mp.ThrottleMetaData.Enabled {
			if mp.ThrottleMetaData.Expiry < time.Now().Unix() {
				mp.ThrottleMetaData.Expiry = time.Now().Unix() + int64(mp.ThrottleMetaData.RateLimitPeriod)
				mp.ThrottleMetaData.SamplePatten = nil
				mp.ThrottleMetaData.Rate = 1
			} else {
				//Throttle Block
				mp.ThrottleMetaData.Rate++
				if len(mp.ThrottleMetaData.SamplePatten) >= mp.ThrottleMetaData.SampleCount {
					mp.ThrottleMetaData.SamplePatten[mp.ThrottleMetaData.Rate % mp.ThrottleMetaData.SampleCount] = key
				} else {
					mp.ThrottleMetaData.SamplePatten = append(mp.ThrottleMetaData.SamplePatten, key)
				}
				if mp.ThrottleMetaData.Rate > mp.ThrottleMetaData.RateLimit {
					mp.ThrottleMetaData.DisabledWrite = true
					mp.ThrottleMetaData.DisabledTime = time.Now().Unix() + int64(mp.ThrottleMetaData.RateLimitPeriod * 60)
					logrus.Errorf("[Persister] Rate Limit: Critical high create metric rate - %d, sample - %v, dropping packet", mp.ThrottleMetaData.Rate ,mp.ThrottleMetaData.SamplePatten)
					return nil, ErrCreateRateLimit
				}
			}
		}
		keydata = GenHashKey(okey)
		_, err := mp.DB.Get(append([]byte("skey:"),  keydata...), nil)
		if err == nil {
			return nil, errors.New("collision")
		} else if err != leveldb.ErrNotFound {
			return nil, err
		}
		err = mp.DB.Put([]byte(key), []byte(keydata), nil)
		if err != nil {
			return nil, err
		}
		err = mp.DB.Put(append([]byte("skey:"),  keydata...), []byte(okey), nil)
		if err != nil {
			return nil, err
		}
	}
	return keydata, err
}

func (mp *LevelMap) GetAggregationMethod(mapKey []byte) ([]byte, error) {
	k := append([]byte("aggr:"), mapKey...)
	return mp.DB.Get(k, nil)
}

func (mp *LevelMap) PutAggregationMethod(mapKey, method []byte) (error) {
	k := append([]byte("aggr:"), mapKey...)
	return mp.DB.Put(k, method, nil)
}

func (mp *LevelMap) GetSchema(mapKey []byte) ([]byte, error) {
	k := append([]byte("schema:"), mapKey...)
	return mp.DB.Get(k, nil)
}

func (mp *LevelMap) PutSchema(mapKey, method []byte) (error) {
	k := append([]byte("schema:"), mapKey...)
	return mp.DB.Put(k, method, nil)
}

func (mp *LevelMap) PutLogPosition(count uint64) (error) {
	k := []byte("writelogpos")
	v := []byte(strconv.FormatUint(count, 10))
	return mp.DB.Put(k, v, nil)
}

func (mp *LevelMap) GetLogPosition() (uint64, error) {
	val , err := mp.DB.Get([]byte("writelogpos"), nil)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(val), 10, 64)
	
}

func (mp *LevelMap) EnableThrottle() {
	mp.ThrottleMetaData.Lock()
	defer mp.ThrottleMetaData.Unlock()
	logrus.Println("Enabling throttle")
	mp.ThrottleMetaData.Enabled = true
}

func (mp *LevelMap) DisableThrottle() {
	mp.ThrottleMetaData.Lock()
	defer mp.ThrottleMetaData.Unlock()
	logrus.Println("Disabling throttle")
	mp.ThrottleMetaData.Enabled = false
}

func (mp *LevelMap) ModifyThrottleLimit(limit int) {
	mp.ThrottleMetaData.Lock()
	defer mp.ThrottleMetaData.Unlock()
	mp.ThrottleMetaData.RateLimit = limit
}

func (mp *LevelMap) ModifyRateLimitPeriod(period int) {
	mp.ThrottleMetaData.Lock()
	defer mp.ThrottleMetaData.Unlock()
	mp.ThrottleMetaData.RateLimitPeriod = period
}

func (mp *LevelMap) Close() {
	mp.DB.Close()
}
