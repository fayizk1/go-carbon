package persister

import (
	"log"
	"path"
	"strconv"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/fayizk1/go-carbon/points"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
)

const SHARD_CACHE_SIZE = 20 << 20 

type Shard struct {
	DB *leveldb.DB
}

func Newshard(basepath, shardname string) (*Shard, error) {
	options := &leveldb_opt.Options{
		BlockCacheCapacity : SHARD_CACHE_SIZE,
		Filter:             leveldb_filter.NewBloomFilter(15),
	}
	storage, err := leveldb.OpenFile(path.Join(basepath, shardname), options)
	if err != nil {
		return nil, err
	}
	shard := new(Shard)
	shard.DB = storage
	return shard, nil
}

func (this *Shard) Write(batch *leveldb.Batch) error {
	return this.DB.Write(batch, nil)	
}

func (this *Shard) Get(name []byte) ([]byte, error) {
	return this.DB.Get(name, nil)	
}

func (this *Shard) RangeScan(start, end, keyname []byte) []points.Point {
	var metrics []points.Point
	iter := this.DB.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	for iter.Next() {
		t := ExtractTs(iter.Key())
		value := iter.Value()
		s, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			log.Println("Shard: Error - Unable to parse value", err)
			continue
		}
		metrics = append(metrics, points.Point{Timestamp: t, Value: s})
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Println("Shard: Unable to iterate", err)
		return nil
	}
	return metrics
}

func (this *Shard) Close() {
	this.DB.Close()
}
