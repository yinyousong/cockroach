// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"fmt"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	storeTimeSeriesNameFmt = "cr.store.%d.%s"
)

// NodeStatusRecorder is used to periodically persist the status of a node as a
// set of time series data.
type NodeStatusRecorder struct {
	*NodeStatusMonitor
	clock         *hlc.Clock
	lastDataCount int
}

// NewNodeStatusRecorder instantiates a recorder for the supplied monitor.
func NewNodeStatusRecorder(monitor *NodeStatusMonitor, clock *hlc.Clock) *NodeStatusRecorder {
	return &NodeStatusRecorder{
		NodeStatusMonitor: monitor,
		clock:             clock,
	}
}

// GetTimeSeriesData returns a slice of interesting TimeSeriesData from the
// encapsulated NodeStatusMonitor.
func (nsr *NodeStatusRecorder) GetTimeSeriesData() []proto.TimeSeriesData {
	data := make([]proto.TimeSeriesData, 0, nsr.lastDataCount)
	nsr.VisitStoreMonitors(func(ssm *StoreStatusMonitor) {
		now := nsr.clock.PhysicalNow()
		ssr := storeStatusRecorder{ssm, now}
		data = append(data, ssr.recordInt("livebytes", ssr.stats.LiveBytes))
		data = append(data, ssr.recordInt("keybytes", ssr.stats.KeyBytes))
		data = append(data, ssr.recordInt("valbytes", ssr.stats.ValBytes))
		data = append(data, ssr.recordInt("intentbytes", ssr.stats.IntentBytes))
		data = append(data, ssr.recordInt("livecount", ssr.stats.LiveCount))
		data = append(data, ssr.recordInt("keycount", ssr.stats.KeyCount))
		data = append(data, ssr.recordInt("valcount", ssr.stats.ValCount))
		data = append(data, ssr.recordInt("intentcount", ssr.stats.IntentCount))
		data = append(data, ssr.recordInt("intentage", ssr.stats.IntentAge))
		data = append(data, ssr.recordInt("gcbytesage", ssr.stats.GCBytesAge))
		data = append(data, ssr.recordInt("lastupdatenanos", ssr.stats.LastUpdateNanos))
		data = append(data, ssr.recordInt("ranges", ssr.rangeCount))
	})
	nsr.lastDataCount = len(data)
	return data
}

// storeStatusRecorder is a helper class for recording time series datapoints
// from a single StoreStatusMonitor.
type storeStatusRecorder struct {
	*StoreStatusMonitor
	timestampNanos int64
}

// recordInt records a single int value from the StoreStatusMonitor as a
// proto.TimeSeriesData object.
func (ssr *storeStatusRecorder) recordInt(name string, data int64) proto.TimeSeriesData {
	return proto.TimeSeriesData{
		Name: fmt.Sprintf(storeTimeSeriesNameFmt, ssr.ID, name),
		Datapoints: []*proto.TimeSeriesDatapoint{
			intDatapoint(ssr.timestampNanos, data),
		},
	}
}

// intDatapoint quickly generates an integer-valued datapoint.
func intDatapoint(timestamp, val int64) *proto.TimeSeriesDatapoint {
	return &proto.TimeSeriesDatapoint{
		TimestampNanos: timestamp,
		IntValue:       gogoproto.Int64(val),
	}
}
