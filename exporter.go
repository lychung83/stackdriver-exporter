// Package exporter provides a way to export data from opencensus to multiple GCP projects.
//
// General assumptions or requirements when using this exporter.
// 1. The basic unit of data is a view.Data with only a single view.Row. We define it as a separate
//    type called RowData.
// 2. We can inspect each RowData to tell whether this RowData is applicable for this exporter.
// 3. For RowData that is applicable to this exporter, we require that
// 3.1. Any view associated to RowData corresponds to a stackdriver metric, and it is already
//      defined for all GCP projects.
// 3.2. RowData has correcponding GCP projects, and we can determine its ID.
// 3.3. After trimming labels and tags, configuration of all view data matches that of corresponding
//      stackdriver metric
package exporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"go.opencensus.io/stats/view"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
)

// wrapper of functions and methods we use in monitoring API. Test may change these variables for
// mocking purpose.
var (
	createTimeSeries = (*monitoring.MetricClient).CreateTimeSeries
	newMetricClient  = monitoring.NewMetricClient
)

// RowDataNonApplicableError is used to tell that given row data is not applicable to the exporter.
// See GetProjectID of Options for more detail.
var RowDataNonApplicableError = errors.New("row data is not applicable to the exporter, so it will be ignored")

// StatsExporter is the exporter that can be registered to opencensus. A StatsExporter object must
// be created by NewStatsExporter().
type StatsExporter struct {
	ctx    context.Context
	client *monitoring.MetricClient
	opts   *Options

	// copy of some option values which may be modified by exporter.
	getProjectID   func(*RowData) (string, error)
	onError        func(error, ...*RowData)
	makeResource   func(map[string]string) (*monitoredrespb.MonitoredResource, error)
	projectKeyName string

	// mu protects access to projDataMap
	mu sync.Mutex
	// per-project data of exporter
	projDataMap map[string]*projectData
}

// Options designates various parameters used by stats exporter. Default value of fields in Options
// are valid for use.
type Options struct {
	// ClientOptions designates options for creating metric client, especially credentials for
	// RPC calls
	ClientOptions []option.ClientOption

	// options for bundles amortizing export requests. Note that a bundle is created for each
	// project. When not provided, default values in bundle package are used.
	BundleDelayThreshold time.Duration
	BundleCountThreshold int

	// callback functions provided by user.

	// GetProjectID is used to filter whether given row data can be applicable to this exporter
	// and if so, it also determines the projectID of given row data. If
	// RowDataNonApplicableError is returned, then the row data is not applicable to this
	// exporter, and it will be silently ignored. Any other error is reported via OnError. When
	// GetProjectID is not set, all row data will be considered non applicable to this exporter.
	GetProjectID func(*RowData) (projectID string, err error)
	// OnError is used to report any error happened while exporting view data fails. Whenever
	// this function is called, it's guaranteed that at least one row data is also passed to
	// OnError. Row data passed to OnError must not be modified. When OnError is not set, all
	// errors happened on exporting are ignored.
	OnError func(error, ...*RowData)
	// MakeResource creates stackdriver resource from labels. Labels passed to this function is
	// union of default labels and tags provided by view data, including exported labels. This
	// function must not modify labels. Any error returned by this function will be considered
	// error when exporting row data to stackdriver. When MakeResource is not set, global
	// resource is used.
	MakeResource func(labels map[string]string) (*monitoredrespb.MonitoredResource, error)

	// options concerning labels.

	// DefaultLabels store default value of some labels. Labels in DefaultLabels need not be
	// specified in tags of view data. It's acceptable to have default labels and tags of view
	// overlapping labels. In this case value in tag is used. Default labels are used for labels
	// those are constant throughout exporte usage, like version number of the running program.
	DefaultLabels map[string]string
	// UnexportedLabels contains key of labels that will not be exported stackdriver. Typical
	// uses of unexported labels will be either that marks project ID, or that's used only for
	// constructing resource.
	UnexportedLabels []string
}

// default values for options
func defaultGetProjectID(rd *RowData) (string, error) {
	return "", RowDataNonApplicableError
}

func defaultOnError(err error, rds ...*RowData) {}

func defaultMakeResource(labels map[string]string) (*monitoredrespb.MonitoredResource, error) {
	return &monitoredrespb.MonitoredResource{Type: "global"}, nil
}

// NewStatsExporter creates a StatsExporter object. If opts is nil, default values are used. Once
// call to NewStatsExporter is made, any fields in opts must not be modified at all. ctx will also
// be used throughout entire exporter operation when making RPC call.
func NewStatsExporter(ctx context.Context, opts *Options) (*StatsExporter, error) {
	if opts == nil {
		opts = &Options{}
	}
	client, err := newMetricClient(ctx, opts.ClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create a metric client: %v", err)
	}

	e := &StatsExporter{
		ctx:         ctx,
		client:      client,
		opts:        opts,
		projDataMap: make(map[string]*projectData),
	}

	// We don't want to modify user-supplied options, so save default options directly in
	// exporter.
	if opts.GetProjectID != nil {
		e.getProjectID = opts.GetProjectID
	} else {
		e.getProjectID = defaultGetProjectID
	}
	if opts.OnError != nil {
		e.onError = opts.OnError
	} else {
		e.onError = defaultOnError
	}
	if opts.MakeResource != nil {
		e.makeResource = opts.MakeResource
	} else {
		e.makeResource = defaultMakeResource
	}

	return e, nil
}

// RowData represents a single row in view data. This is our unit of computation. We use a single
// row instead of view data because a view data consists of multiple rows, and each row may belong
// to different projects.
type RowData struct {
	View       *view.View
	Start, End time.Time
	Row        *view.Row
}

// ExportView is the method called by opencensus to export view data.
func (e *StatsExporter) ExportView(vd *view.Data) {
	for _, row := range vd.Rows {
		rd := &RowData{
			View:  vd.View,
			Start: vd.Start,
			End:   vd.End,
			Row:   row,
		}
		e.exportRowData(rd)
	}
}

// exportRowData exports a single row data.
func (e *StatsExporter) exportRowData(rd *RowData) {
	projID, err := e.getProjectID(rd)
	if err != nil {
		// We ignore non-applicable RowData.
		if err != RowDataNonApplicableError {
			newErr := fmt.Errorf("failed to get project ID on row data with view %q: %v", rd.View.Name, err)
			e.onError(newErr, rd)
		}
		return
	}
	pd := e.getProjectData(projID)
	switch err := pd.bundler.Add(rd, 1); err {
	case nil:
	case bundler.ErrOversizedItem:
		go pd.uploadRowData(rd)
	default:
		newErr := fmt.Errorf("failed to add row data with view %q to bundle for project %q: %v", rd.View.Name, projID, err)
		e.onError(newErr, rd)
	}
}

func (e *StatsExporter) getProjectData(projectID string) *projectData {
	e.mu.Lock()
	defer e.mu.Unlock()
	if pd, ok := e.projDataMap[projectID]; ok {
		return pd
	}

	pd := e.newProjectData(projectID)
	e.projDataMap[projectID] = pd
	return pd
}

// Close flushes and closes the exporter. Close must be called after the exporter is unregistered
// and no further calls to ExportView() are made. Once Close() is returned no further access to the
// exporter is allowed in any form.
func (e *StatsExporter) Close() error {
	e.mu.Lock()
	for _, pd := range e.projDataMap {
		pd.bundler.Flush()
	}
	e.mu.Unlock()

	if err := e.client.Close(); err != nil {
		return fmt.Errorf("failed to close the metric client: %v", err)
	}
	return nil
}
