package reader

import (
	"fmt"
	"log"
	"time"
	"sort"
	"io/ioutil"
	"strconv"
	"net/http"
	"encoding/json"
	b64 "encoding/base64"
	"github.com/Sirupsen/logrus"
	"github.com/fayizk1/go-carbon/persister"
	"github.com/fayizk1/go-carbon/points"
	"github.com/fayizk1/go-carbon/cache"
)

type PointsData struct {
	Pts persister.Points `json:"points"`
	Step int `json:"step""`
}

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
		http.HandleFunc("/setforcenode", serveSetForceNode)		
		http.HandleFunc("/throttle", throttleHandler)
		http.HandleFunc("/cleardisabledwrite", clearDisabledWrite)
		http.HandleFunc("/deletedata", deleteData)
		http.HandleFunc("/deleteshard", deleteShard)
		http.HandleFunc("/lastdate", serveLastDate)
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

func serveSetForceNode(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	var t []string
	content, _ := ioutil.ReadAll(r.Body)
	log.Println(string(content))
	err := json.Unmarshal(content, &t)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	err = h.persistor.ForceSetIndex(name, t)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write([]byte("Success"))
}


func clearDisabledWrite(w http.ResponseWriter, r *http.Request) {
	h.persistor.ClearDisabledWrite()
}

func deleteData(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	dec, err := b64.URLEncoding.DecodeString(name)
	if err != nil {
		http.Error(w, "BAD REQUEST[START]", 400)
		return
	}
	name = string(dec)
	err = h.persistor.DeleteData(name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func deleteShard(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	archive := r.FormValue("archive")
	apos, err := strconv.Atoi(archive)
	if err != nil {
		http.Error(w, "BAD REQUEST[START]", 400)
		return
	}
	h.persistor.DeleteShard(apos, name)
}

func throttleHandler(w http.ResponseWriter, r *http.Request) {
	method := r.FormValue("method")
	switch method {
	case "toggle":
		key := r.FormValue("key")
		if key == "enable" {
			h.persistor.EnableThrottle()
		} else if key == "disable" {
			h.persistor.DisableThrottle()
		} else {
			http.Error(w, "BAD REQUEST[START]", 400)
			return
		}
	default:
		http.Error(w, "BAD REQUEST", 400)
	}
}

func serveLastDate(w http.ResponseWriter, r *http.Request) {
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
	w.Write([]byte(strconv.FormatInt(h.persistor.LastDate(name, start, end), 10)))
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
	pdata, step, _, aggM := h.persistor.GetRangeData(name, start, end, false)
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
		data, _ := json.Marshal(PointsData{Pts : pdata, Step : step})
		w.Write(data)
		return
	}
	cData := make(map[int64]float64)
	for _, pts := range cacheQ.InFlightData {
		for _, item := range pts.Data {
			if item.Timestamp > end || item.Timestamp < start {
				continue
			}
			tpoint := item.Timestamp - (item.Timestamp % int64(step))
			switch aggM {
			case "sum":
				if cv, ok := cData[tpoint]; ok {
					cData[tpoint] = cv + item.Value
				} else {
					cData[tpoint] = item.Value
				}
			case "max" :
				if cv, ok := cData[tpoint];ok {
					if item.Value > cv {
						cData[tpoint] =  item.Value
					}
				} else {
					cData[tpoint] = item.Value
				}
			case "min":
				if cv, ok := cData[tpoint];ok {
					if item.Value < cv {
						cData[tpoint] =  item.Value
					}
				} else {
					cData[tpoint] = item.Value
				}
			case "average":
				if cv, ok := cData[tpoint];ok {
					cData[tpoint] =  (cv + item.Value) / 2
				} else {
					cData[tpoint] = item.Value
				}
			}
			/*cPoints = append(cPoints, points.Point{
				Timestamp : (item.Timestamp - (item.Timestamp % int64(step))),
				Value : item.Value,
			})*/
		}
	}
	if cacheQ.CacheData != nil {
		for _, item := range cacheQ.CacheData.Data {
                        if item.Timestamp > end || item.Timestamp < start {
				continue
			}
			tpoint := item.Timestamp - (item.Timestamp % int64(step))
			switch aggM {
			case "sum":
				if cv, ok := cData[tpoint]; ok {
					cData[tpoint] = cv + item.Value
				} else {
					cData[tpoint] = item.Value
				}
			case "max" :
				if cv, ok := cData[tpoint];ok {
					if item.Value > cv {
						cData[tpoint] =  item.Value
					}
				} else {
					cData[tpoint] = item.Value
				}
			case "min":
				if cv, ok := cData[tpoint];ok {
					if item.Value < cv {
						cData[tpoint] =  item.Value
					}
				} else {
					cData[tpoint] = item.Value
				}
			case "average":
				if cv, ok := cData[tpoint];ok {
					cData[tpoint] =  (cv + item.Value) / 2
				} else {
					cData[tpoint] = item.Value
				}
			}	
		}
	}
	var cPoints []points.Point
	for k, v := range cData {
		cPoints = append(cPoints, points.Point{
			Timestamp : k,
			Value : v,
		})
	}
	pdata = append(pdata, cPoints...)
	sort.Sort(pdata)
	data, _ := json.Marshal(PointsData{Pts : pdata, Step : step})
	w.Write(data)
	return
}

