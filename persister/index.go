package persister

import (
	"path"
	"errors"
	"strings"
	"encoding/json"
	leveldb_filter "github.com/syndtr/goleveldb/leveldb/filter"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/gobwas/glob"
	"github.com/Sirupsen/logrus"
)

const INDEX_CACHE_SIZE = 40 << 20

type IndexType struct {
	Isleaf bool `json:"isbool"`
	FullName string `json:"fullname"`
	LastNode string `json:"lastnode"`
}

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
		if curname == "" {
			var rmembers []string
			rdata, err := idx.DB.Get([]byte("."), nil)
			if err != leveldb.ErrNotFound && err != nil {
				return err
			} else if err == leveldb.ErrNotFound {
				rdata = []byte("[]")
			}
			if err = json.Unmarshal(rdata, &rmembers); err != nil {
				return err
			}
			if !InSlice(segnames[i], rmembers) {
				rmembers = append(rmembers, segnames[i])
			}
			rval, _ := json.Marshal(rmembers)
			err = idx.DB.Put([]byte("."), rval, nil)
			if err != nil {
				return err
			}
			curname = segnames[i]
		} else {
			curname = strings.Join([]string{curname, segnames[i]},".")
		}
		if (i+1) < len(segnames) {
			child = segnames[i+1]
		} else {
			child = ""
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

func (idx *LevelIndex) GetChildrenSpecial(resolved, unResolved []string) ([]IndexType, error) {
	var parent string
	var AllChildren []IndexType
	if len(unResolved) == 0 {
		if len(resolved) == 0 {
			return nil, errors.New("Empty resolved")
		}
		fullName := strings.Join(resolved, ".")
		sdata, err := idx.DB.Get([]byte(fullName), nil)
		if err != nil && err != leveldb.ErrNotFound {
			logrus.Println(err)
			return nil, err
		} else if err == leveldb.ErrNotFound {
			return  []IndexType{IndexType{Isleaf : true, FullName : fullName, LastNode : resolved[len(resolved)-1]},}, nil
		}
		var tsibilings []string
		if err = json.Unmarshal(sdata, &tsibilings); err != nil || len(tsibilings) == 0{
			return []IndexType{IndexType{Isleaf : true, FullName : fullName, LastNode : resolved[len(resolved)-1]},}, nil
		}
		return []IndexType{IndexType{Isleaf : false, FullName : fullName, LastNode : resolved[len(resolved)-1]}, }, nil 
	}
	

	if len(resolved) == 0 {
		parent = "."
	} else {
		parent = strings.Join(resolved, ".")
	}
	data, err := idx.DB.Get([]byte(parent), nil)
	if err != nil {
		return nil, err
	}
	var sibilings []string
	if err = json.Unmarshal(data, &sibilings); err != nil {
		return nil, err
	}
	if len(unResolved[0]) == 0 {
		return nil , errors.New("Empty pattern")
	}
	pattern := unResolved[0]
	if len(unResolved) == 1 {
		if !strings.ContainsAny(pattern, "*{}[]")  {
			pattern = pattern + "*"
		}
	}
	g := glob.MustCompile(pattern)
	for i := range sibilings {
		if !g.Match(sibilings[i]) {
			continue
		}
		rkey :=  make([]string, len(resolved))
		urkey := make([]string, len(unResolved) - 1)
		copy(rkey, resolved)
		rkey = append(rkey, sibilings[i])
		copy(urkey, unResolved[1:])
		tempChildren, err := idx.GetChildrenSpecial(rkey, urkey)
		if err != nil {
			logrus.Println("Unable to solve keys", err)
		}
		if tempChildren == nil {
			continue
		}
		AllChildren = append(AllChildren, tempChildren...)
	}
	return AllChildren, nil
}

func (idx *LevelIndex) GetChildren(name string) ([]IndexType, error) {
	var sibilings []string
	var AllChildren []IndexType
	segnames := strings.Split(name, ".")
	if strings.ContainsAny(name, "*[]{}"){
		return idx.GetChildrenSpecial(nil, segnames)
	} else {
		var parentNodes, lastNode string
		if len(segnames) == 1 {
			parentNodes = "."
			lastNode = segnames[0]
		} else {
			parentNodes = strings.Join(segnames[:len(segnames)-1], ".")
			lastNode = segnames[len(segnames)-1]
		}
		data, err := idx.DB.Get([]byte(parentNodes), nil)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &sibilings); err != nil {
			return nil, err
		}
		var validSiblings []string
		for j := range sibilings {
			if strings.HasPrefix(sibilings[j], lastNode) {
				validSiblings = append(validSiblings, sibilings[j])
			}
		}
		for k := range validSiblings {
			fullName := ""
			if parentNodes == "." {
				fullName = validSiblings[k]
			} else {
				fullName = parentNodes + "." + validSiblings[k]
			}
			data, err := idx.DB.Get([]byte(fullName), nil)
			if err != nil && err != leveldb.ErrNotFound {
				return nil, err
			} else if err == leveldb.ErrNotFound {
				AllChildren = append(AllChildren, IndexType{Isleaf : true, FullName : fullName, LastNode : validSiblings[k]})
				continue
			}
			var nodeChildren []string
			err = json.Unmarshal(data, &nodeChildren)
			if err != nil  || len(nodeChildren) == 0 {
				AllChildren = append(AllChildren, IndexType{Isleaf : true, FullName : fullName, LastNode : validSiblings[k]})
				continue
			} else {
				AllChildren = append(AllChildren, IndexType{Isleaf : false, FullName : fullName, LastNode : validSiblings[k]})
				continue
			}
		}
	}
	return AllChildren, nil
}

func (idx *LevelIndex) Close() {
	idx.DB.Close()
}

