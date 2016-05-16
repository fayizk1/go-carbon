package persister

import (
	"path"
	"strings"
	"encoding/json"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb"
)

const INDEX_CACHE_SIZE = 40 << 20

type LevelIndex struct {
	Path string
	DB *leveldb.DB
}

func NewIndex(basepath string) *LevelIndex {
	options := &leveldb_opt.Options{
		BlockCacheCapacity : INDEX_CACHE_SIZE,
		Filter:             leveldb_filter.NewBloomFilter(15),
	}
	path := path.Join(basepath, "carbon.index")
	db, err := leveldb.OpenFile(path, options)
	if err != nil {
		panic(err)
	}
	return &LevelIndex{Path : path, DB: db}
}

func (idx *LevelIndex) CreateIndex(name string) error {
	_, err := idx.DB.Get([]byte(name), nil)
	if err == nil {
		return nil
	} else if err != leveldb.ErrNotFound {
		return err
	}
	segnames := strings.Split(name,".")
	var child, curname string
	for i := range segnames {
		var sibilings []string
		curname = curname + "." + segnames[i]
		if (i+1) < len(segnames) {
			child = segnames[i+1]
		}
		data, err := idx.DB.Get([]byte(curname), nil)
		if err != leveldb.ErrNotFound && err != nil {
			return err
		} else if err == leveldb.ErrNotFound {
			data = []byte("[]")
		}
		if err = json.Unmarshal(data, &sibilings); err != nil {
			return err
		}
		if len(child ) > 0 {
			if !InSlice(child, sibilings) {
				sibilings = append(sibilings, child)
			}
		}
		val, _ := json.Marshal(sibilings)
		err = idx.DB.Put([]byte(curname), val, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *LevelIndex) GetChildren(name string) ([]string, error) {
	data, err := idx.DB.Get([]byte(name), nil)
	if err != nil {
		return nil, err
	}
	var sibilings []string
	if err = json.Unmarshal(data, &sibilings); err != nil {
		return nil, err
	}
	return sibilings, nil
}

func (idx *LevelIndex) Close() {
	idx.DB.Close()
}

