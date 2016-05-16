package cache

import "github.com/fayizk1/go-carbon/points"

// Query request from carbonlink
type Query struct {
	Metric       string
	Wait         chan bool        // close after finish collect reply
	CacheData    *points.Points   // from cache
	InFlightData []*points.Points // from confirm tracker
}

// NewQuery create Query instance
func NewQuery(metric string) *Query {
	return &Query{
		Metric: metric,
		Wait:   make(chan bool),
	}
}
