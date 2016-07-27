package persister

import (
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"
	"path"
	"encoding/json"
	"github.com/Sirupsen/logrus"
	"github.com/fayizk1/go-carbon/helper"
	"github.com/fayizk1/go-carbon/points"
	"github.com/fayizk1/go-carbon/persister/replication"
)

type LevelStore struct {
	helper.Stoppable
	updateOperations    uint32
	commitedPoints      uint32
 	in                  chan *points.Points
	confirm             chan *points.Points
	rplIn                  chan *points.Points
	schemas             *WhisperSchemas
	aggregation         *WhisperAggregation
	metricInterval      time.Duration // checkpoint interval
	workersCount        int
	rootPath            string
	graphPrefix         string
	created             uint32 // counter
	sparse              bool
	maxUpdatesPerSecond int
	mockStore           func(p *LevelStore, values *points.Points, replication bool)
	shards              map[string]Shard
	Map                 *LevelMap
	archives            *Archives
	index               *LevelIndex
	rplog               *replication.LevelReplicationLog
	rthread             *replication.LevelReplicationThread
}

type Archives struct {
	sync.Mutex
	list map[string]*Archive
	rootPath string
	Map                 *LevelMap
}

func NewArchives(rootPath string, Map *LevelMap) *Archives {
	return &Archives {
		list : make(map[string]*Archive),
		rootPath : rootPath,
		Map: Map,
	}
}

func (ars *Archives) Get(pos int) *Archive{
	ars.Lock()
	defer ars.Unlock()
	arName := fmt.Sprintf("arch%d", pos)
	ar, ok := ars.list[arName]
	if !ok {
		ar = NewArchive(path.Join(ars.rootPath, arName), ars.Map)
		ars.list[arName] = ar
	}
	return ar
}

func NewLevelStore(rootPath string, schemas *WhisperSchemas, aggregation *WhisperAggregation, in chan *points.Points,
	confirm chan *points.Points, peerlist []string, rserver string, rpasswordHash string, Logpath string,
	RateLimit, RateLimitPeriod, DisablePeriod int, MailServer string, MailFrom string, MailTO []string, MailUsername, MailPassword string) *LevelStore {
	Map := NewMap(rootPath, RateLimit, RateLimitPeriod, 3, DisablePeriod, MailServer, MailFrom, MailTO, MailUsername, MailPassword)
	archives := NewArchives(rootPath, Map)
	index := NewIndex(rootPath)
	rplog := replication.NewReplicationLog(Logpath)
	rplIn := make(chan *points.Points)
	rthread := replication.NewReplicationThread(rplog , peerlist,  rserver, rpasswordHash, rplIn)
	return &LevelStore{
		in:                  in,
		confirm:             confirm,
		schemas:             schemas,
		aggregation:         aggregation,
		metricInterval:      time.Minute,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
		Map:                 Map,
		archives:            archives,
		index:               index,
		rplog :              rplog,
		rthread:           rthread,
		rplIn:             rplIn,
	}
}

// SetGraphPrefix for internal cache metrics
func (p *LevelStore) SetGraphPrefix(prefix string) {
	p.graphPrefix = prefix
}

// SetMaxUpdatesPerSecond enable throttling
func (p *LevelStore) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
}

// GetMaxUpdatesPerSecond returns current throttling speed
func (p *LevelStore) GetMaxUpdatesPerSecond() int {
	return p.maxUpdatesPerSecond
}

// SetWorkers count
func (p *LevelStore) SetWorkers(count int) {
	p.workersCount = count
}

// SetSparse creation
func (p *LevelStore) SetSparse(sparse bool) {
	p.sparse = sparse
}

// SetMetricInterval sets doChekpoint interval
func (p *LevelStore) SetMetricInterval(interval time.Duration) {
	p.metricInterval = interval
}

// Stat sends internal statistics to cache
func (p *LevelStore) Stat(metric string, value float64) {
	p.in <- points.OnePoint(
		fmt.Sprintf("%spersister.%s", p.graphPrefix, metric),
		value,
		time.Now().Unix(),
	)
}

func store(p *LevelStore, values *points.Points, replication bool) {
	shortKey, err := p.Map.GetShortKey(values.Metric, true)
	if err != nil && err != ErrCreateRateLimit {
		logrus.Errorf("[persister] unable to get short key for %s, Error: %q", values.Metric, err)
		return
	}
	err = p.index.CreateIndex(values.Metric)
	if err != nil {
		logrus.Errorf("[persister] Unable to create index for %s, Error - %s", values.Metric, err.Error())
		return
	}
	if p.confirm != nil {
		defer func() { p.confirm <- values }()
	}
	aggM, err := p.Map.GetAggregationMethod(shortKey)
	if err != nil {
		aggr := p.aggregation.match(values.Metric)
		if aggr == nil {
			logrus.Errorf("[persister] No storage aggregation defined for %s", values.Metric)
			return
		}
		aggM = []byte(aggr.aggregationMethodStr)
		err = p.Map.PutAggregationMethod(shortKey, aggM)
		if err != nil {
			logrus.Errorf("[persister] Unable to write aggr map for %s", values.Metric)
		}
	}
	retnM, err := p.Map.GetSchema(shortKey)
	if err != nil {
		schema, ok := p.schemas.Match(values.Metric)
		if !ok {
			logrus.Errorf("[persister] No storage schema defined for %s", values.Metric)
			return
		}
		retnM = []byte(schema.retentionStr)
		err = p.Map.PutSchema(shortKey, retnM)
		if err != nil {
			logrus.Errorf("[persister] Unable to write schema map for %s", values.Metric)
		}
	}
	retentions, err := ParseRetentionDefs(string(retnM))
	if err != nil {
		logrus.Errorf("[persister] Unable to parse retention for %s", values.Metric)
		return
	}
	if !replication {
		logData, _ := json.Marshal(values)
		_, err = p.rplog.WriteLog(logData)
		if err != nil {
			logrus.Errorf("[persister] Unable to write log-  %v", err)
			return
		}
	}
	for i, r := range retentions {
		ar := p.archives.Get(i)
		err = ar.Store(shortKey, values, int64(r.SecondsPerPoint()) , time.Now().Unix() - int64(r.NumberOfPoints() * r.SecondsPerPoint()), string(aggM))
		if err != nil {
			logrus.Errorf("[persister] Unable to write into %s - Archive %d", values.Metric, i)
		}
	}
	atomic.AddUint32(&p.commitedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[persister] UpdateMany %s recovered: %s", values.Metric, r)
		}
	}()
}

func (p *LevelStore) worker(in chan *points.Points, exit chan bool) {
	storeFunc := store
	if p.mockStore != nil {
		storeFunc = p.mockStore
	}

LOOP:
	for {
		select {
		case <-exit:
			break LOOP
		case values, ok := <-p.rplIn:
			if !ok {
				break LOOP
			}
			storeFunc(p, values, true)
		case values, ok := <-in:
			if !ok {
				break LOOP
			}
			storeFunc(p, values, false)
		}
	}
}

func (p *LevelStore) shuffler(in chan *points.Points, out [](chan *points.Points), exit chan bool) {
	workers := uint32(len(out))

LOOP:
	for {
		select {
		case <-exit:
			break LOOP
		case values, ok := <-in:
			if !ok {
				break LOOP
			}
			index := crc32.ChecksumIEEE([]byte(values.Metric)) % workers
			out[index] <- values
		}
	}

	for _, ch := range out {
		close(ch)
	}
}

// save stat
func (p *LevelStore) doCheckpoint() {
	updateOperations := atomic.LoadUint32(&p.updateOperations)
	commitedPoints := atomic.LoadUint32(&p.commitedPoints)
	atomic.AddUint32(&p.updateOperations, -updateOperations)
	atomic.AddUint32(&p.commitedPoints, -commitedPoints)

	created := atomic.LoadUint32(&p.created)
	atomic.AddUint32(&p.created, -created)

	logrus.WithFields(logrus.Fields{
		"updateOperations": int(updateOperations),
		"commitedPoints":   int(commitedPoints),
		"created":          int(created),
	}).Info("[persister] doCheckpoint()")

	p.Stat("updateOperations", float64(updateOperations))
	p.Stat("commitedPoints", float64(commitedPoints))
	if updateOperations > 0 {
		p.Stat("pointsPerUpdate", float64(commitedPoints)/float64(updateOperations))
	} else {
		p.Stat("pointsPerUpdate", 0.0)
	}

	p.Stat("created", float64(created))

}

// stat timer
func (p *LevelStore) statWorker(exit chan bool) {
	ticker := time.NewTicker(p.metricInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-exit:
			break LOOP
		case <-ticker.C:
			go p.doCheckpoint()
		}
	}
}

func throttleChan(in chan *points.Points, ratePerSec int, exit chan bool) chan *points.Points {
	out := make(chan *points.Points, cap(in))

	delimeter := ratePerSec
	chunk := 1

	if ratePerSec > 1000 {
		minRemainder := ratePerSec

		for i := 100; i < 1000; i++ {
			if ratePerSec%i < minRemainder {
				delimeter = i
				minRemainder = ratePerSec % delimeter
			}
		}

		chunk = ratePerSec / delimeter
	}

	step := time.Duration(1e9/delimeter) * time.Nanosecond

	var onceClose sync.Once

	throttleWorker := func() {
		var p *points.Points
		var ok bool

		defer onceClose.Do(func() { close(out) })

		// start flight
		throttleTicker := time.NewTicker(step)
		defer throttleTicker.Stop()

	LOOP:
		for {
			select {
			case <-throttleTicker.C:
				for i := 0; i < chunk; i++ {
					select {
					case p, ok = <-in:
						if !ok {
							break LOOP
						}
					case <-exit:
						break LOOP
					}
					out <- p
				}
			case <-exit:
				break LOOP
			}
		}
	}

	go throttleWorker()

	return out
}

// Start worker
func (p *LevelStore) Start() error {

	return p.StartFunc(func() error {
		p.rthread.Start()
		p.Go(func(exitChan chan bool) {
			p.statWorker(exitChan)
		})
		p.WithExit(func(exitChan chan bool) {
			inChan := p.in
			readerExit := exitChan
			if p.maxUpdatesPerSecond > 0 {
				inChan = throttleChan(inChan, p.maxUpdatesPerSecond, exitChan)
				readerExit = nil // read all before channel is closed
			}

			if p.workersCount <= 1 { // solo worker
				p.Go(func(e chan bool) {
					p.worker(inChan, readerExit)
				})
			} else {
				var channels [](chan *points.Points)

				for i := 0; i < p.workersCount; i++ {
					ch := make(chan *points.Points, 32)
					channels = append(channels, ch)
					p.Go(func(e chan bool) {
						p.worker(ch, nil)
					})
				}
				p.Go(func(e chan bool) {
					p.shuffler(inChan, channels, readerExit)
				})
			}
		})
		return nil
	})
}

func (p *LevelStore) FindNodes(key string) []IndexType {
	children, err := p.index.GetChildren(key)
	if err  != nil {
		logrus.Errorf("[persister] Unable to find nodes %s", err.Error())
		if len(children) == 0 {
			return []IndexType{}
		}
	}
	return children
}

func (p *LevelStore) GetRangeData(name string, start, end int64, sorting bool) (Points, int, int, string) {
	shortKey, err := p.Map.GetShortKey(name, false)
	if err != nil {
		logrus.Errorf("[persister] unable to get short key for %s", name)
		return nil, 0, 0, "average"
	}
	aggM, err := p.Map.GetAggregationMethod(shortKey)
	if err != nil {
		logrus.Errorf("[persister] Unable to read agg method, setting default s %s", err.Error())
		aggM = []byte("average")
	}
	retnM, err := p.Map.GetSchema(shortKey)
	if err != nil {
		logrus.Errorf("[persister] Unable to get schema map for %s %v", name, err)
		return nil, 0, 0, string(aggM)
	}
	retentions, err := ParseRetentionDefs(string(retnM))
	if err != nil {
		logrus.Errorf("[persister] Unable to parse retention for %s", name)
		return nil, 0, 0, string(aggM)
	}
	var step, arcpos, npoints int
	for i, r := range retentions {
		arcpos = i
		step = r.SecondsPerPoint()
		npoints = r.SecondsPerPoint()
		if int64(time.Now().Unix() - int64(r.NumberOfPoints() * r.SecondsPerPoint())) > start {
			step = r.SecondsPerPoint()
			npoints = r.SecondsPerPoint()
			break
		}
	}
	ar := p.archives.Get(arcpos)
	return ar.GetData(start, end, shortKey, sorting), step, npoints, string(aggM)
}

func (p *LevelStore) LastDate(name string, start, end int64) int64 {
	shortKey, err := p.Map.GetShortKey(name, false)
	if err != nil {
		logrus.Errorf("[persister] unable to get short key for %s", name)
		return 0
	}
	retnM, err := p.Map.GetSchema(shortKey)
	if err != nil {
		logrus.Errorf("[persister] Unable to get schema map for %s %v", name, err)
		return 0
	}
	retentions, err := ParseRetentionDefs(string(retnM))
	if err != nil {
		logrus.Errorf("[persister] Unable to parse retention for %s", name)
		return 0
	}
	var arcpos int
	for i, r := range retentions {
		arcpos = i
		if int64(time.Now().Unix() - int64(r.NumberOfPoints() * r.SecondsPerPoint())) < end {
			break
		}
	}
	ar := p.archives.Get(arcpos)
	return ar.LastDate(start, end, shortKey)
}

func (p *LevelStore) DeleteData(name string) (error) {
	shortKey, err := p.Map.GetShortKey(name, false)
	if err != nil {
		logrus.Errorf("[persister] unable to get short key for %s", name)
		return err
	}
	retnM, err := p.Map.GetSchema(shortKey)
	if err != nil {
		logrus.Errorf("[persister] Unable to get schema map for %s %v", name, err)
		return err
	}
	retentions, err := ParseRetentionDefs(string(retnM))
	if err != nil {
		logrus.Errorf("[persister] Unable to parse retention for %s", name)
		return err
	}
	for i, _ := range retentions {
		ar := p.archives.Get(i)
		err := ar.DeleteData(shortKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *LevelStore) DeleteShard(archive int, name string) {
	p.archives.Get(archive).DeleteShard(name)
}

func (p *LevelStore) EnableThrottle() {
	p.Map.EnableThrottle()
}

func (p *LevelStore) DisableThrottle() {
	p.Map.DisableThrottle()
}

func (p *LevelStore) ClearDisabledWrite() {
	p.Map.ClearDisabledWrite()
}
