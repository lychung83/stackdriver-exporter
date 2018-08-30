package exporter

import (
	"fmt"
	"testing"

	"go.opencensus.io/stats/view"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
)

// This file contains actual tests.

// TestProjectClassifyNoError tests that exporter can recognize and distribute incoming data by
// its project.
func TestProjectClassifyNoError(t *testing.T) {
	viewData1 := &view.Data{
		View:  view1,
		Start: startTime1,
		End:   endTime1,
		Rows:  []*view.Row{view1row1, view1row2},
	}
	viewData2 := &view.Data{
		View:  view2,
		Start: startTime2,
		End:   endTime2,
		Rows:  []*view.Row{view2row1},
	}

	getProjectID := func(rd *RowData) (string, error) {
		switch rd.Row {
		case view1row1, view2row1:
			return project1, nil
		case view1row2:
			return project2, nil
		default:
			return "", unrecognizedDataError
		}
	}

	exp, errStore := newMockExp(t, &Options{GetProjectID: getProjectID})
	exp.ExportView(viewData1)
	exp.ExportView(viewData2)

	wantRowData := map[string][]*RowData{
		project1: []*RowData{
			{view1, startTime1, endTime1, view1row1},
			{view2, startTime2, endTime2, view2row1},
		},
		project2: []*RowData{
			{view1, startTime1, endTime1, view1row2},
		},
	}
	checkErrStorage(t, errStore, nil)
	checkExpProjData(t, exp, wantRowData)
}

// TestProjectClassifyError tests that exporter can properly handle errors while classifying
// incoming data by its project.
func TestProjectClassifyError(t *testing.T) {
	viewData1 := &view.Data{
		View:  view1,
		Start: startTime1,
		End:   endTime1,
		Rows:  []*view.Row{view1row1, view1row2},
	}
	viewData2 := &view.Data{
		View:  view2,
		Start: startTime2,
		End:   endTime2,
		Rows:  []*view.Row{view2row1, view2row2},
	}

	getProjectID := func(rd *RowData) (string, error) {
		switch rd.Row {
		case view1row1, view2row2:
			return project1, nil
		case view1row2:
			return "", RowDataNotApplicableError
		case view2row1:
			return "", invalidDataError
		default:
			return "", unrecognizedDataError
		}
	}

	exp, errStore := newMockExp(t, &Options{GetProjectID: getProjectID})
	exp.ExportView(viewData1)
	exp.ExportView(viewData2)

	wantErrRdCheck := []errRowDataCheck{
		{
			errPrefix: "failed to get project ID",
			errSuffix: invalidDataError.Error(),
			rds:       []*RowData{{view2, startTime2, endTime2, view2row1}},
		},
	}
	wantRowData := map[string][]*RowData{
		project1: []*RowData{
			{view1, startTime1, endTime1, view1row1},
			{view2, startTime2, endTime2, view2row2},
		},
	}
	checkErrStorage(t, errStore, wantErrRdCheck)
	checkExpProjData(t, exp, wantRowData)
}

//
func TestUploadNoError(t *testing.T) {
	pd, cl, errStore := newMockUploader(t, &Options{})
	rd := []*RowData{
		{view1, startTime1, endTime1, view1row1},
		{view1, startTime1, endTime1, view1row2},
		{view1, startTime1, endTime1, view1row3},
		{view2, startTime2, endTime2, view2row1},
		{view2, startTime2, endTime2, view2row2},
	}
	pd.uploadRowData(rd)

	checkErrStorage(t, errStore, nil)
	wantClData := [][]int64{
		{1, 2, 3},
		{4, 5},
	}
	checkMetricClient(t, cl, wantClData)
}

// TestUploadTimeSeriesMakeError tests that errors while creating time series are properly handled.
func TestUploadTimeSeriesMakeError(t *testing.T) {
	makeResource := func(rd *RowData) (*monitoredrespb.MonitoredResource, error) {
		if rd.Row == view1row2 {
			return nil, invalidDataError
		}
		return defaultMakeResource(rd)
	}
	pd, cl, errStore := newMockUploader(t, &Options{MakeResource: makeResource})
	rd := []*RowData{
		{view1, startTime1, endTime1, view1row1},
		{view1, startTime1, endTime1, view1row2},
		{view1, startTime1, endTime1, view1row3},
		// This row data is invalid, so it will trigger inconsistent data error.
		{view2, startTime2, endTime2, invalidRow},
		{view2, startTime2, endTime2, view2row1},
		{view2, startTime2, endTime2, view2row2},
	}
	pd.uploadRowData(rd)

	wantErrRdCheck := []errRowDataCheck{
		{
			errPrefix: "failed to construct resource",
			errSuffix: invalidDataError.Error(),
			rds:       []*RowData{{view1, startTime1, endTime1, view1row2}},
		}, {
			errPrefix: "inconsistent data found in view",
			errSuffix: metric2name,
			rds:       []*RowData{{view2, startTime2, endTime2, invalidRow}},
		},
	}
	checkErrStorage(t, errStore, wantErrRdCheck)

	wantClData := [][]int64{
		{1, 3, 4},
		{5},
	}
	checkMetricClient(t, cl, wantClData)
}

// TestUploadTimeSeriesMakeError tests that exporter can handle error on metric client's time
// series create RPC call.
func TestUploadWithMetricClientError(t *testing.T) {
	pd, cl, errStore := newMockUploader(t, &Options{})
	cl.addReturnErrs(invalidDataError)
	rd := []*RowData{
		{view1, startTime1, endTime1, view1row1},
		{view1, startTime1, endTime1, view1row2},
		{view1, startTime1, endTime1, view1row3},
		{view2, startTime2, endTime2, view2row1},
		{view2, startTime2, endTime2, view2row2},
	}
	pd.uploadRowData(rd)

	wantErrRdCheck := []errRowDataCheck{
		{
			errPrefix: "RPC call to create time series failed",
			errSuffix: invalidDataError.Error(),
			rds: []*RowData{
				{view1, startTime1, endTime1, view1row1},
				{view1, startTime1, endTime1, view1row2},
				{view1, startTime1, endTime1, view1row3},
			},
		},
	}
	checkErrStorage(t, errStore, wantErrRdCheck)

	wantClData := [][]int64{
		{1, 2, 3},
		{4, 5},
	}
	checkMetricClient(t, cl, wantClData)
}

// TestMakeResource tests that exporter can create monitored resource dynamically.
func TestMakeResource(t *testing.T) {
	makeResource := func(rd *RowData) (*monitoredrespb.MonitoredResource, error) {
		switch rd.Row {
		case view1row1:
			return resource1, nil
		case view1row2:
			return resource2, nil
		default:
			return nil, unrecognizedDataError
		}
	}
	pd, cl, errStore := newMockUploader(t, &Options{MakeResource: makeResource})
	rd := []*RowData{
		{view1, startTime1, endTime1, view1row1},
		{view1, startTime1, endTime1, view1row2},
	}
	pd.uploadRowData(rd)
	checkErrStorage(t, errStore, nil)
	checkMetricClient(t, cl, [][]int64{{1, 2}})

	tsArr := cl.reqs[0].TimeSeries
	for i, wantResource := range []*monitoredrespb.MonitoredResource{resource1, resource2} {
		if resource := tsArr[i].Resource; resource != wantResource {
			t.Errorf("%d-th time series resource got: %#v, want: %#v", i, resource, wantResource)
		}
	}
}

// TestMakeLabel tests that exporter can correctly handle label manipulation process, including
// merging default label with tags, and removing unexported labels.
func TestMakeLabel(t *testing.T) {
	opts := &Options{
		DefaultLabels: map[string]string{
			label1name: value4,
			label4name: value5,
		},
		UnexportedLabels: []string{label3name, label5name},
	}
	pd, cl, errStore := newMockUploader(t, opts)
	rd := []*RowData{
		{view1, startTime1, endTime1, view1row1},
		{view2, startTime2, endTime2, view2row1},
	}
	pd.uploadRowData(rd)
	checkErrStorage(t, errStore, nil)
	checkMetricClient(t, cl, [][]int64{{1, 4}})

	wantLabels1 := map[string]string{
		label1name: value4,
		label4name: value5,
	}
	wantLabels2 := map[string]string{
		// default value for key1 is suppressed, and value defined in tag of view2row1 is
		// used.
		label1name: value1,
		label2name: value2,
		label4name: value5,
	}
	tsArr := cl.reqs[0].TimeSeries
	for i, wantLabels := range []map[string]string{wantLabels1, wantLabels2} {
		prefix := fmt.Sprintf("%d-th time series labels mismatch", i+1)
		checkLabels(t, prefix, tsArr[i].Metric.Labels, wantLabels)
	}
}
