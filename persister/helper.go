package persister

import (
	"log"
	"bytes"
	"encoding/binary"
	"crypto/md5"
	"github.com/fayizk1/go-carbon/points"
)

type ShardData struct {
	Key string
	Value     float64
}

type KeyNode struct {
	isleaf bool `json:"isbool"`
	children []string `json:"children"`
}

type Points []points.Point

func (pt Points) Len() int {
	return len(pt)
}

func (pt Points) Less(i, j int) bool {
	return pt[i].Timestamp < pt[j].Timestamp;
}

func (pt Points) Swap(i, j int) {
	pt[i], pt[j] = pt[j], pt[i]
}

func GenHashKey(key string) []byte {
	hash := md5.New()
	hash.Write([]byte(key))
	return hash.Sum(nil)
	
}

func GenMetricKey(key []byte, ts int64) []byte {
	buf := new(bytes.Buffer)
	buf.Write(key)
	binary.Write(buf, binary.BigEndian, ts)
	return buf.Bytes()
}

func ExtractTs(key []byte) int64 {
	n := len(key)
	if n != 24 {
		log.Println("Unable to extract key len: ", n)
			return 0
	}
	buf := new(bytes.Buffer)
	buf.Write(key[16:])
	var val int64
	binary.Read(buf, binary.BigEndian, &val)
	return val
}

func InSlice(s string,sl []string ) bool {
	for _, v := range sl{
		if v == s {
			return true
		}
	}
	return false
}
