package exporter

import (
	"fmt"
	"time"

	"go.opencensus.io/tag"
	"google.golang.org/api/support/bundler"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

// maximum number of time series that stackdriver accepts. Only test may change this value.
var MaxTimeSeriesPerUpload = 200

// projectData contain per-project data in exporter. It should be created by newProjectData()
type projectData struct {
	parent    *StatsExporter
	projectID string
	// We make bundler for each project because call to monitoring RPC can be grouped only in
	// project level
	bndler expBundler
}

// We wrap bundler and its maker for testing purpose.
type expBundler interface {
	Add(interface{}, int) error
	Flush()
}

var newExpBundler = defaultNewExpBundler

// Since options in bundler are directly set to its fields and interface does not allow any fields,
// we put option set-up process inside bundler's maker.
func defaultNewExpBundler(uploader func(interface{}), delayThreshold time.Duration, countThreshold int) expBundler {
	bndler := bundler.NewBundler((*RowData)(nil), uploader)

	// Set options for bundler if they are provided by users.
	if 0 < delayThreshold {
		bndler.DelayThreshold = delayThreshold
	}
	if 0 < countThreshold {
		bndler.BundleCountThreshold = countThreshold
	}

	return bndler
}

func (e *StatsExporter) newProjectData(projectID string) *projectData {
	pd := &projectData{
		parent:    e,
		projectID: projectID,
	}

	pd.bndler = newExpBundler(pd.uploadRowData, e.opts.BundleDelayThreshold, e.opts.BundleCountThreshold)
	return pd
}

// uploadRowData is called by bundler to upload row data, and report any error happened meanwhile.
func (pd *projectData) uploadRowData(bundle interface{}) {
	exp := pd.parent
	rds := bundle.([]*RowData)

	// reqRds contains RowData objects those are uploaded to stackdriver at given iteration.
	// It's main usage is for error reporting. For actual uploading operation, we use req.
	// remainingRds are RowData that has not been processed at all.
	var reqRds, remainingRds []*RowData
	for ; len(rds) != 0; rds = remainingRds {
		var req *monitoringpb.CreateTimeSeriesRequest
		req, reqRds, remainingRds = pd.makeReq(rds)
		if req == nil {
			// no need to perform RPC call for empty set of requests.
			continue
		}
		if err := exp.client.CreateTimeSeries(exp.ctx, req); err != nil {
			newErr := fmt.Errorf("RPC call to create time series failed for project %s: %v", pd.projectID, err)
			// We pass all row data not successfully uploaded.
			exp.onError(newErr, reqRds...)
		}
	}
}

// makeReq creates a request that's suitable to be passed to create time series RPC call.
//
// reqRds contains rows those are contained in req. Main use of reqRds is to be returned to users if
// creating time series failed. (We don't want users to investigate structure of timeseries.)
// remainingRds contains rows those are not used at all in makeReq because of the length limitation
// or request. Another call of makeReq() with remainigRds will handle (some) rows in them. When req
// is nil, then there's nothing to request and reqRds will also contain nothing.
//
// Some rows in rds may fail while converting them to time series, and in that case makeReq() calls
// exporter's onError() directly, not propagating errors to the caller.
func (pd *projectData) makeReq(rds []*RowData) (req *monitoringpb.CreateTimeSeriesRequest, reqRds, remainingRds []*RowData) {
	exp := pd.parent
	timeSeries := []*monitoringpb.TimeSeries{}

	var i int
	var rd *RowData
	for i, rd = range rds {
		pt := newPoint(rd.View, rd.Row, rd.Start, rd.End)
		if pt.Value == nil {
			err := fmt.Errorf("inconsistent data found in view %s", rd.View.Name)
			pd.parent.onError(err, rd)
			continue
		}
		resource, err := exp.makeResource(rd)
		if err != nil {
			newErr := fmt.Errorf("failed to construct resource of view %s: %v", rd.View.Name, err)
			pd.parent.onError(newErr, rd)
			continue
		}

		ts := &monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{
				Type:   rd.View.Name,
				Labels: exp.makeLabels(rd.Row.Tags),
			},
			Resource: resource,
			Points:   []*monitoringpb.Point{pt},
		}
		// Growing timeseries and reqRds are done at same time.
		timeSeries = append(timeSeries, ts)
		reqRds = append(reqRds, rd)
		// Don't grow timeseries over the limit.
		if len(timeSeries) == MaxTimeSeriesPerUpload {
			break
		}
	}

	// Since i is the last index processed, remainingRds should start from i+1.
	remainingRds = rds[i+1:]
	if len(timeSeries) == 0 {
		req = nil
	} else {
		req = &monitoringpb.CreateTimeSeriesRequest{
			Name:       fmt.Sprintf("projects/%s", pd.projectID),
			TimeSeries: timeSeries,
		}
	}
	return req, reqRds, remainingRds
}

// makeLables constructs label that's ready for being uploaded to stackdriver.
func (e *StatsExporter) makeLabels(tags []tag.Tag) map[string]string {
	opts := e.opts
	labels := make(map[string]string, len(opts.DefaultLabels)+len(tags))
	for key, val := range opts.DefaultLabels {
		labels[key] = val
	}
	// If there's overlap When combining exporter's default label and tags, values in tags win.
	for _, tag := range tags {
		labels[tag.Key.Name()] = tag.Value
	}
	// Some labels are not for exporting.
	for _, key := range opts.UnexportedLabels {
		delete(labels, key)
	}
	return labels
}
