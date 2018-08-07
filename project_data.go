package exporter

import (
	"fmt"

	"google.golang.org/api/support/bundler"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

// maximum number of time series that stackdriver accepts.
const MaxTimeSeriesPerUpload = 200

// projectData contain per-project data in exporter.
type projectData struct {
	parent    *StatsExporter
	projectID string
	// We make bundler for each project because call to monitoring RPC can be grouped only in
	// project level
	bundler *bundler.Bundler
}

func (e *StatsExporter) newProjectData(projectID string) *projectData {
	pd := &projectData{
		parent:    e,
		projectID: projectID,
	}

	bundler := bundler.NewBundler((*RowData)(nil), pd.uploadRowData)

	// Set options for bundler if they are provided by users.
	opts := e.opts
	if 0 < opts.BundleDelayThreshold {
		bundler.DelayThreshold = opts.BundleDelayThreshold
	}
	if 0 < opts.BundleCountThreshold {
		bundler.BundleCountThreshold = opts.BundleCountThreshold
	}

	pd.bundler = bundler
	return pd
}

// uploadRowData is called by bundler to upload row data, and report any error happened meanwhile.
func (pd *projectData) uploadRowData(bundle interface{}) {
	exp := pd.parent

	var reqRds, remainingRds []*RowData
	for rds := bundle.([]*RowData); len(rds) != 0; rds = remainingRds {
		var req *monitoringpb.CreateTimeSeriesRequest
		req, reqRds, remainingRds = pd.makeReq(rds)
		if req == nil {
			continue
		}
		if err := createTimeSeries(exp.client, exp.ctx, req); err != nil {
			newErr := fmt.Errorf("RPC call to create time series failed for project %q: %v", pd.projectID, err)
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
// exporter's onError() directly, not propagating errors to caller.
func (pd *projectData) makeReq(rds []*RowData) (req *monitoringpb.CreateTimeSeriesRequest, reqRds, remainingRds []*RowData) {
	exp := pd.parent
	timeSeries := []*monitoringpb.TimeSeries{}

	var i int
	var rd *RowData
	for i, rd = range rds {
		pt := newPoint(rd.View, rd.Row, rd.Start, rd.End)
		if pt == nil {
			err := fmt.Errorf("view %q has inconsistency", rd.View.Name)
			pd.parent.onError(err, rd)
			continue
		}
		// Construct resource out of all labels including unexported ones.
		labels := exp.newRawLabels(rd.Row.Tags)
		resource, userErr := exp.makeResource(labels)
		if userErr != nil {
			newErr := fmt.Errorf("failed to construct resource of view %q using user-provided MakeResource(): %v", rd.View.Name, userErr)
			pd.parent.onError(newErr, rd)
			continue
		}

		// After resource is constructed, we only leave exported labels.
		for _, key := range exp.opts.UnexportedLabels {
			delete(labels, key)
		}
		ts := &monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{
				Type:   rd.View.Name,
				Labels: labels,
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
