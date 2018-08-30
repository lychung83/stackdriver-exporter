package exporter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	gax "github.com/googleapis/gax-go"
	"google.golang.org/api/option"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

// This file defines various mocks for testing, and checking functions for mocked data. We mock
// metric client and bundler because their actions involves RPC calls or non-deterministic behavior.

func init() {
	newMetricClient = mockNewMetricClient
	newExpBundler = mockNewExpBundler
}

// We define mock Client.

type mockMetricClient struct {
	// returnErrs holds predefined error values to return. Each errors in returnErrs are
	// returned per each CreateTimeSeries() call. If all errors in returnErrs are used, all
	// other CreateTimeSeries calls will return nil.
	returnErrs []error
	// reqs saves all incoming requests.
	reqs []*monitoringpb.CreateTimeSeriesRequest
}

func (cl *mockMetricClient) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	cl.reqs = append(cl.reqs, req)
	// Check returnErrs and if not empty, return the first error from it.
	if len(cl.returnErrs) == 0 {
		return nil
	}
	err := cl.returnErrs[0]
	// delete the returning error.
	cl.returnErrs = cl.returnErrs[1:]
	return err
}

func (cl *mockMetricClient) Close() error {
	return nil
}

func (cl *mockMetricClient) addReturnErrs(errs ...error) {
	cl.returnErrs = append(cl.returnErrs, errs...)
}

func mockNewMetricClient(_ context.Context, _ ...option.ClientOption) (metricClient, error) {
	return &mockMetricClient{}, nil
}

// checkMetricClient checks all recorded requests in mock metric client. We only compare int64
// values of the time series. To make this work, we assigned different int64 values for all valid
// rows in the test.
func checkMetricClient(t *testing.T, cl *mockMetricClient, wantReqsValues [][]int64) {
	reqs := cl.reqs
	reqsLen, wantReqsLen := len(reqs), len(wantReqsValues)
	if reqsLen != wantReqsLen {
		t.Errorf("number of requests got: %d, want %d", reqsLen, wantReqsLen)
		return
	}
	for i := 0; i < reqsLen; i++ {
		prefix := fmt.Sprintf("%d-th request mismatch", i+1)
		tsArr := reqs[i].TimeSeries
		wantTsValues := wantReqsValues[i]
		tsArrLen, wantTsArrLen := len(tsArr), len(wantTsValues)
		if tsArrLen != wantTsArrLen {
			t.Errorf("%s: number of time series got: %d, want: %d", prefix, tsArrLen, wantTsArrLen)
			continue
		}
		for j := 0; j < tsArrLen; j++ {
			// This is how monitoring API stores the int64 value.
			tsVal := tsArr[j].Points[0].Value.Value.(*monitoringpb.TypedValue_Int64Value).Int64Value
			wantTsVal := wantTsValues[j]
			if tsVal != wantTsVal {
				t.Errorf("%s: Value got: %d, want: %d", prefix, tsVal, wantTsVal)
			}
		}
	}
}

// We define mock bundler.

type mockBundler struct {
	// rowDataArr saves all incoming RowData to the bundler.
	rowDataArr []*RowData
}

func (b *mockBundler) Add(rowData interface{}, _ int) error {
	b.rowDataArr = append(b.rowDataArr, rowData.(*RowData))
	return nil
}

func (b *mockBundler) Flush() {}

func mockNewExpBundler(_ func(interface{}), _ time.Duration, _ int) expBundler {
	return &mockBundler{}
}

// We define a storage for all errors happened in export operation.

type errStorage struct {
	errRds []errRowData
}

type errRowData struct {
	err error
	rds []*RowData
}

// onError records any incoming error and accompanying RowData array. This method is passed to the
// exporter to record errors.
func (e *errStorage) onError(err error, rds ...*RowData) {
	e.errRds = append(e.errRds, errRowData{err, rds})
}

// errRowDataCheck contains data for checking content of error storage.
type errRowDataCheck struct {
	errPrefix, errSuffix string
	rds                  []*RowData
}

// checkErrStorage checks content of error storage. For returned errors, we check prefix and suffix.
func checkErrStorage(t *testing.T, errStore *errStorage, wantErrRdCheck []errRowDataCheck) {
	errRds := errStore.errRds
	gotLen, wantLen := len(errRds), len(wantErrRdCheck)
	if gotLen != wantLen {
		t.Errorf("number of reported errors: %d, want: %d", gotLen, wantLen)
		return
	}
	for i := 0; i < gotLen; i++ {
		prefix := fmt.Sprintf("%d-th reported error mismatch", i+1)
		errRd, wantErrRd := errRds[i], wantErrRdCheck[i]
		errStr := errRd.err.Error()
		if errPrefix := wantErrRd.errPrefix; !strings.HasPrefix(errStr, errPrefix) {
			t.Errorf("%s: error got: %q, want: prefixed by %q", prefix, errStr, errPrefix)
		}
		if errSuffix := wantErrRd.errSuffix; !strings.HasSuffix(errStr, errSuffix) {
			t.Errorf("%s: error got: %q, want: suffiexd by %q", prefix, errStr, errSuffix)
		}
		if err := checkRowDataArr(errRd.rds, wantErrRd.rds); err != nil {
			t.Errorf("%s: RowData array mismatch: %v", prefix, err)
		}
	}
}

func checkRowDataArr(rds, wantRds []*RowData) error {
	rdLen, wantRdLen := len(rds), len(wantRds)
	if rdLen != wantRdLen {
		return fmt.Errorf("number row data got: %d, want: %d", rdLen, wantRdLen)
	}
	for i := 0; i < rdLen; i++ {
		if err := checkRowData(rds[i], wantRds[i]); err != nil {
			return fmt.Errorf("%d-th row data mismatch: %v", i+1, err)
		}
	}
	return nil
}

func checkRowData(rd, wantRd *RowData) error {
	if rd.View != wantRd.View {
		return fmt.Errorf("View got: %s, want: %s", rd.View.Name, wantRd.View.Name)
	}
	if rd.Start != wantRd.Start {
		return fmt.Errorf("Start got: %v, want: %v", rd.Start, wantRd.Start)
	}
	if rd.End != wantRd.End {
		return fmt.Errorf("End got: %v, want: %v", rd.End, wantRd.End)
	}
	if rd.Row != wantRd.Row {
		return fmt.Errorf("Row got: %v, want: %v", rd.Row, wantRd.Row)
	}
	return nil
}

// newMockExp creates mock expoter and error storage storing all errors. Caller need not set
// opts.OnError.
func newMockExp(t *testing.T, opts *Options) (*StatsExporter, *errStorage) {
	errStore := &errStorage{}
	opts.OnError = errStore.onError
	exp, err := NewStatsExporter(ctx, opts)
	if err != nil {
		t.Fatalf("creating exporter failed: %v", err)
	}
	return exp, errStore
}

// checkExpProjData checks all data passed to the bundler by bundler.Add().
func checkExpProjData(t *testing.T, exp *StatsExporter, wantProjData map[string][]*RowData) {
	wantProj := map[string]bool{}
	for proj := range wantProjData {
		wantProj[proj] = true
	}
	for proj := range exp.projDataMap {
		if !wantProj[proj] {
			t.Errorf("project in exporter's project data not wanted: %s", proj)
		}
	}

	for proj, wantRds := range wantProjData {
		pd, ok := exp.projDataMap[proj]
		if !ok {
			t.Errorf("wanted project not found in exporter's project data: %v", proj)
			continue
		}
		// We know that bundler is mocked, so we can check the data in it.
		if err := checkRowDataArr(pd.bndler.(*mockBundler).rowDataArr, wantRds); err != nil {
			t.Errorf("RowData array mismatch for project %s: %v", proj, err)
		}
	}
}

// newMockUploader creates objects to test behavior of projectData.uploadRowData. Other uses are not
// recommended.
func newMockUploader(t *testing.T, opts *Options) (*projectData, *mockMetricClient, *errStorage) {
	exp, errStore := newMockExp(t, opts)
	pd := exp.newProjectData(project1)
	cl := exp.client.(*mockMetricClient)
	return pd, cl, errStore
}

// checkLabels checks data in labels.
func checkLabels(t *testing.T, prefix string, labels, wantLabels map[string]string) {
	for labelName, value := range labels {
		wantValue, ok := wantLabels[labelName]
		if !ok {
			t.Errorf("%s: label name in time series not wanted: %s", prefix, labelName)
			continue
		}
		if value != wantValue {
			t.Errorf("%s: value for label name %s got: %s, want: %s", prefix, labelName, value, wantValue)
		}
	}
	for wantLabelName := range wantLabels {
		if _, ok := labels[wantLabelName]; !ok {
			t.Errorf("%s: wanted label name not found in time series: %s", prefix, wantLabelName)
		}
	}
}
