package exporter

import (
	"time"

	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

func newPoint(v *view.View, row *view.Row, start, end time.Time) *monitoringpb.Point {
	switch v.Aggregation.Type {
	case view.AggTypeLastValue:
		return newGaugePoint(v, row, end)
	default:
		return newCumulativePoint(v, row, start, end)
	}
}

func newCumulativePoint(v *view.View, row *view.Row, start, end time.Time) *monitoringpb.Point {
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{
			StartTime: &timestamppb.Timestamp{
				Seconds: start.Unix(),
				Nanos:   int32(start.Nanosecond()),
			},
			EndTime: &timestamppb.Timestamp{
				Seconds: end.Unix(),
				Nanos:   int32(end.Nanosecond()),
			},
		},
		Value: newTypedValue(v, row),
	}
}

func newGaugePoint(v *view.View, row *view.Row, end time.Time) *monitoringpb.Point {
	gaugeTime := &timestamppb.Timestamp{
		Seconds: end.Unix(),
		Nanos:   int32(end.Nanosecond()),
	}
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{
			EndTime: gaugeTime,
		},
		Value: newTypedValue(v, row),
	}
}

func newTypedValue(vd *view.View, r *view.Row) *monitoringpb.TypedValue {
	switch v := r.Data.(type) {
	case *view.CountData:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{
			Int64Value: v.Value,
		}}
	case *view.SumData:
		switch vd.Measure.(type) {
		case *stats.Int64Measure:
			return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{
				Int64Value: int64(v.Value),
			}}
		case *stats.Float64Measure:
			return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{
				DoubleValue: v.Value,
			}}
		}
	case *view.DistributionData:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{
			DistributionValue: &distributionpb.Distribution{
				Count:                 v.Count,
				Mean:                  v.Mean,
				SumOfSquaredDeviation: v.SumOfSquaredDev,
				// TODO(songya): uncomment this once Stackdriver supports min/max.
				// Range: &distributionpb.Distribution_Range{
				//      Min: v.Min,
				//      Max: v.Max,
				// },
				BucketOptions: &distributionpb.Distribution_BucketOptions{
					Options: &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
						ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
							Bounds: vd.Aggregation.Buckets,
						},
					},
				},
				BucketCounts: v.CountPerBucket,
			},
		}}
	case *view.LastValueData:
		switch vd.Measure.(type) {
		case *stats.Int64Measure:
			return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{
				Int64Value: int64(v.Value),
			}}
		case *stats.Float64Measure:
			return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{
				DoubleValue: v.Value,
			}}
		}
	}
	return nil
}

// newRawLables combines exporter's default label and tags. When there's any overlap, values in tags
// win.
func (e *StatsExporter) newRawLabels(tags []tag.Tag) map[string]string {
	opts := e.opts
	labels := make(map[string]string, len(opts.DefaultLabels)+len(tags))
	for key, val := range opts.DefaultLabels {
		labels[key] = val
	}
	for _, tag := range tags {
		labels[tag.Key.Name()] = tag.Value
	}
	return labels
}
