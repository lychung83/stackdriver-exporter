package exporter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
)

// This file defines various data needed for testing.

func init() {
	// For testing convenience, we reduce maximum time series that metric client accepts.
	MaxTimeSeriesPerUpload = 3
}

const (
	label1name = "key_1"
	label2name = "key_2"
	label3name = "key_3"
	label4name = "key_4"
	label5name = "key_5"

	value1 = "value_1"
	value2 = "value_2"
	value3 = "value_3"
	value4 = "value_4"
	value5 = "value_5"
	value6 = "value_6"

	metric1name = "metric_1"
	metric1desc = "this is metric 1"
	metric2name = "metric_2"
	metric2desc = "this is metric 2"

	project1 = "project-1"
	project2 = "project-2"
)

var (
	ctx = context.Background()

	// This error is used for test to catch some error happpened.
	invalidDataError = errors.New("invalid data")
	// This error is used for unexpected error.
	unrecognizedDataError = errors.New("unrecognized data")

	key1 = getKey(label1name)
	key2 = getKey(label2name)
	key3 = getKey(label3name)

	view1 = &view.View{
		Name:        metric1name,
		Description: metric1desc,
		TagKeys:     nil,
		Measure:     stats.Int64(metric1name, metric1desc, stats.UnitDimensionless),
		Aggregation: view.Sum(),
	}
	view2 = &view.View{
		Name:        metric2name,
		Description: metric2desc,
		TagKeys:     []tag.Key{key1, key2, key3},
		Measure:     stats.Int64(metric2name, metric2desc, stats.UnitDimensionless),
		Aggregation: view.Sum(),
	}

	// To make verification easy, we require all valid rows should int64 values and all of them
	// must be distinct.
	view1row1 = &view.Row{
		Tags: nil,
		Data: &view.SumData{Value: 1},
	}
	view1row2 = &view.Row{
		Tags: nil,
		Data: &view.SumData{Value: 2},
	}
	view1row3 = &view.Row{
		Tags: nil,
		Data: &view.SumData{Value: 3},
	}
	view2row1 = &view.Row{
		Tags: []tag.Tag{{key1, value1}, {key2, value2}, {key3, value3}},
		Data: &view.SumData{Value: 4},
	}
	view2row2 = &view.Row{
		Tags: []tag.Tag{{key1, value4}, {key2, value5}, {key3, value6}},
		Data: &view.SumData{Value: 5},
	}
	// This Row does not have valid Data field, so is invalid.
	invalidRow = &view.Row{Data: nil}

	startTime1 = endTime1.Add(-10 * time.Second)
	endTime1   = startTime2.Add(-time.Second)
	startTime2 = endTime2.Add(-10 * time.Second)
	endTime2   = time.Now()

	resource1 = &monitoredrespb.MonitoredResource{
		Type: "cloudsql_database",
		Labels: map[string]string{
			"project_id":  project1,
			"region":      "us-central1",
			"database_id": "cloud-SQL-instance-1",
		},
	}
	resource2 = &monitoredrespb.MonitoredResource{
		Type: "gce_instance",
		Labels: map[string]string{
			"project_id":  project2,
			"zone":        "us-east1",
			"database_id": "GCE-instance-1",
		},
	}
)

func getKey(name string) tag.Key {
	key, err := tag.NewKey(name)
	if err != nil {
		panic(fmt.Errorf("key creation failed for key name: %s", name))
	}
	return key
}
