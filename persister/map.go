package persister

import (
	"path"
	"errors"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb"
)

const MAP_CACHE_SIZE = 40 << 20

type LevelMap struct {
	Path string
	DB *leveldb.DB
}

func NewMap(basepath string) (*LevelMap) {
	options := &leveldb_opt.Options{
		BlockCacheCapacity : MAP_CACHE_SIZE,
		Filter:             leveldb_filter.NewBloomFilter(15),
	}
	path := path.Join(basepath, "carbon.map")
	db, err := leveldb.OpenFile(path, options)
	if err != nil {
		panic(err)
	}
	return &LevelMap{Path : path, DB: db}
}

func (mp *LevelMap) GetShortKey(key string, iswrite bool) ([]byte, error) {
	okey := key
	key = "key:" + key
	keydata, err := mp.DB.Get([]byte(key), nil)
	if !iswrite {
		return keydata, err
	}
	if err == leveldb.ErrNotFound {
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
	return keydata, nil
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

func (mp *LevelMap) Close() {
	mp.DB.Close()
}
