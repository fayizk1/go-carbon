package reader

import (
	"fmt"
	"log"
	"time"
	"sort"
	"strconv"
	"net/http"
	"encoding/json"
	"github.com/Sirupsen/logrus"
	"github.com/fayizk1/go-carbon/persister"
	"github.com/fayizk1/go-carbon/points"
	"github.com/fayizk1/go-carbon/cache"
)

type HTTP struct {
	cachequeryChan chan *cache.Query
	persistor *persister.LevelStore
	listen string
}

var h *HTTP

func NewHTTPReader(listen string, cachequeryChan chan *cache.Query, persistor *persister.LevelStore) *HTTP{
	if h != nil {
		return h
	}
	h = &HTTP{
		cachequeryChan : cachequeryChan,
		listen : listen,
		persistor: persistor,
	}
	return h
}

func StartHTTPReader() {
	go func() {
		http.HandleFunc("/queryrange", serveQueryRange)
		http.HandleFunc("/findnodes", serveFindNodes)
		err := http.ListenAndServe(h.listen, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()
}

func serveFindNodes(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("query")
	nodes := h.persistor.FindNodes(query)
	out, _ := json.Marshal(nodes)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(out)
}

func serveQueryRange(w http.ResponseWriter, r *http.Request) {
	start, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("BAD REQUEST[START] -> %v", err), 400)
		return
	}
	end, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("BAD REQUEST[END] -> %v", err), 400)
		return
	}
	name := r.FormValue("name")
	cacheQ := cache.NewQuery(name)
	select {
	case h.cachequeryChan <- cacheQ:
		// pass
	case <-time.After(2 * time.Second):
		logrus.Infof("[httpreader] cache no reciever after 2 sec(timeout)")
		cacheQ = nil // empty reply
	}
	pdata, step, _ := h.persistor.GetRangeData(name, start, end, false)
	if cacheQ != nil {
		select {
		case <-cacheQ.Wait:
			// pass
		case <-time.After(3 * time.Second):
			logrus.Infof("[httpreader] Cache no reply (%s timeout)", 3 * time.Second)
			cacheQ = nil // empty reply
		}
	}
	if cacheQ == nil || (cacheQ.InFlightData == nil && cacheQ.CacheData == nil ){
		sort.Sort(pdata)
		data, _ := json.Marshal(pdata)
		w.Write(data)
		return
	}
	var cPoints []points.Point
	for _, pts := range cacheQ.InFlightData {
		for _, item := range pts.Data {
			cPoints = append(cPoints, points.Point{
				Timestamp : (item.Timestamp - (item.Timestamp % int64(step))),
				Value : item.Value,
			})
		}
	}
	if cacheQ.CacheData != nil {
		for _, q := range cacheQ.CacheData.Data {
			cPoints = append(cPoints, points.Point{
				Timestamp : (q.Timestamp - (q.Timestamp % int64(step))),
				Value :q.Value,
			})
		}
	}
	pdata = append(pdata, cPoints...)
	sort.Sort(pdata)
	data, _ := json.Marshal(pdata)
	w.Write(data)
	return
}
