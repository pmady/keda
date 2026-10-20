package scalers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v2 "k8s.io/api/autoscaling/v2"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

// The files in testdata/opencost are /allocation responses in the shape OpenCost writes
// (core/pkg/protocol HTTPResponse around an AllocationSetRange):
//   - allocation.json: three namespaces (team-a, kube-system, __unmounted__), window 2026-10-18 10:00-11:00 UTC
//   - allocation_team_a.json: the same response as returned for filter=namespace:"team-a"
//   - allocation_empty_set.json: no allocations and no filter, which OpenCost writes as data [{}]
//   - allocation_no_data.json: a filter that matches nothing, which OpenCost writes as data []
//   - error_500.json: the envelope OpenCost sends with HTTP 500
//
// totalCost also includes loadBalancerCost and sharedCost, so it is not the sum of the per-type costs.
const openCostFixtureDir = "testdata/opencost"

// Cost per costType, summed over the allocations in the fixture.
var (
	openCostFixtureAllCosts = map[string]float64{
		"totalCost": 0.121, "cpuCost": 0.07, "gpuCost": 0, "ramCost": 0.04, "pvCost": 0.006, "networkCost": 0,
	}
	openCostFixtureTeamACosts = map[string]float64{
		"totalCost": 0.037, "cpuCost": 0.02, "gpuCost": 0, "ramCost": 0.01, "pvCost": 0.002, "networkCost": 0,
	}
)

func readOpenCostFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(openCostFixtureDir, name))
	require.NoError(t, err)
	return body
}

// newOpenCostServer answers every request with the given status and body.
func newOpenCostServer(t *testing.T, status int, body []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

// openCostMetadata returns valid trigger metadata with the overrides applied and the given keys removed.
func openCostMetadata(overrides map[string]string, without ...string) map[string]string {
	metadata := map[string]string{
		"serverAddress": "http://opencost.opencost:9003",
		"costThreshold": "100",
	}
	for k, v := range overrides {
		metadata[k] = v
	}
	for _, k := range without {
		delete(metadata, k)
	}
	return metadata
}

func newOpenCostTestScaler(t *testing.T, serverAddress string, overrides map[string]string) *openCostScaler {
	t.Helper()
	metadata := openCostMetadata(overrides)
	metadata["serverAddress"] = serverAddress
	scaler, err := NewOpenCostScaler(&scalersconfig.ScalerConfig{TriggerMetadata: metadata})
	require.NoError(t, err)
	return scaler.(*openCostScaler)
}

func TestOpenCostMetadataDefaults(t *testing.T) {
	meta, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: openCostMetadata(nil),
		TriggerIndex:    3,
	})
	require.NoError(t, err)

	assert.Equal(t, "http://opencost.opencost:9003", meta.ServerAddress)
	assert.InDelta(t, 100.0, meta.CostThreshold, 0)
	// TODO: the default window is planned to be replaced by an explicit start and end.
	assert.Equal(t, "1h", meta.Window)
	assert.Equal(t, "namespace", meta.Aggregate)
	assert.Empty(t, meta.Filter)
	assert.Equal(t, "totalCost", meta.CostType)
	assert.InDelta(t, 0.0, meta.ActivationCostThreshold, 0)
	assert.False(t, meta.InverseScaling)
	assert.False(t, meta.UnsafeSsl)
	assert.Equal(t, 3, meta.triggerIndex)
}

func TestOpenCostMetadataExplicitValues(t *testing.T) {
	meta, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{
			"serverAddress":           "https://opencost.example.com",
			"costThreshold":           "0.5",
			"activationCostThreshold": "0.25",
			"window":                  "7d",
			"aggregate":               "pod",
			"filter":                  `namespace:"team-a"`,
			"costType":                "ramCost",
			"unsafeSsl":               "true",
			"inverseScaling":          "true",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "https://opencost.example.com", meta.ServerAddress)
	assert.InDelta(t, 0.5, meta.CostThreshold, 0)
	assert.InDelta(t, 0.25, meta.ActivationCostThreshold, 0)
	assert.Equal(t, "7d", meta.Window)
	assert.Equal(t, "pod", meta.Aggregate)
	assert.Equal(t, `namespace:"team-a"`, meta.Filter)
	assert.Equal(t, "ramCost", meta.CostType)
	assert.True(t, meta.UnsafeSsl)
	assert.True(t, meta.InverseScaling)
}

func TestOpenCostMetadataEnums(t *testing.T) {
	for _, aggregate := range []string{"cluster", "node", "namespace", "controllerKind", "controller", "service", "pod", "container"} {
		t.Run("aggregate "+aggregate, func(t *testing.T) {
			_, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: openCostMetadata(map[string]string{"aggregate": aggregate}),
			})
			assert.NoError(t, err)
		})
	}
	for costType := range openCostFixtureAllCosts {
		t.Run("costType "+costType, func(t *testing.T) {
			_, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: openCostMetadata(map[string]string{"costType": costType}),
			})
			assert.NoError(t, err)
		})
	}
}

func TestOpenCostMetadataValidation(t *testing.T) {
	tests := []struct {
		name        string
		metadata    map[string]string
		errContains string
	}{
		{"missing serverAddress", openCostMetadata(nil, "serverAddress"), "serverAddress"},
		{"missing costThreshold", openCostMetadata(nil, "costThreshold"), "costThreshold"},
		{"costThreshold zero", openCostMetadata(map[string]string{"costThreshold": "0"}), "costThreshold must be a positive number"},
		{"costThreshold negative", openCostMetadata(map[string]string{"costThreshold": "-0.5"}), "costThreshold must be a positive number"},
		{"costThreshold not a number", openCostMetadata(map[string]string{"costThreshold": "ten"}), "costThreshold"},
		{"activationCostThreshold not a number", openCostMetadata(map[string]string{"activationCostThreshold": "ten"}), "activationCostThreshold"},
		{"inverseScaling not a bool", openCostMetadata(map[string]string{"inverseScaling": "maybe"}), "inverseScaling"},
		{"unsafeSsl not a bool", openCostMetadata(map[string]string{"unsafeSsl": "maybe"}), "unsafeSsl"},
		{"unknown aggregate", openCostMetadata(map[string]string{"aggregate": "invalidAggregate"}), "aggregate"},
		{"aggregate is case sensitive", openCostMetadata(map[string]string{"aggregate": "Namespace"}), "aggregate"},
		{"unknown costType", openCostMetadata(map[string]string{"costType": "invalidCost"}), "costType"},
		{"costType is case sensitive", openCostMetadata(map[string]string{"costType": "totalcost"}), "costType"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: tc.metadata})
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.errContains)
		})
	}
}

func TestOpenCostMetadataWindow(t *testing.T) {
	parse := func(window string) error {
		_, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
			TriggerMetadata: openCostMetadata(map[string]string{"window": window}),
		})
		return err
	}

	for _, window := range []string{"1h", "12h", "24h", "168h", "1d", "3d", "7d", "1w", "2w"} {
		t.Run("valid "+window, func(t *testing.T) {
			assert.NoError(t, parse(window))
		})
	}

	for _, window := range []string{"h", "1", "d", "1x", "-1h", "1.5h", "1h30m", "1H", "1hour", "1 h", "h1"} {
		t.Run("invalid "+window, func(t *testing.T) {
			err := parse(window)
			require.Error(t, err)
			assert.ErrorContains(t, err, "invalid window format")
		})
	}

	// The metadata parser trims whitespace, and an empty value falls back to the default window.
	for _, window := range []string{"", " 1h", "1h "} {
		t.Run("normalised to 1h "+window, func(t *testing.T) {
			meta, err := parseOpenCostMetadata(&scalersconfig.ScalerConfig{
				TriggerMetadata: openCostMetadata(map[string]string{"window": window}),
			})
			require.NoError(t, err)
			assert.Equal(t, "1h", meta.Window)
		})
	}

	// TODO: the window regex is planned to be relaxed. These are valid OpenCost windows that the
	// current pattern (^\d+[hdw]$) rejects. Flip them to valid when it changes.
	for _, window := range []string{
		"30m", "today", "yesterday", "week", "lastweek", "month",
		"1700000000,1700003600", "2026-10-01T00:00:00Z,2026-10-02T00:00:00Z",
	} {
		t.Run("valid in OpenCost, rejected today "+window, func(t *testing.T) {
			err := parse(window)
			require.Error(t, err)
			assert.ErrorContains(t, err, "invalid window format")
		})
	}

	// TODO: 0h is an empty window in OpenCost (start equals end). It is accepted today and is
	// expected to be rejected once the window validation is reworked.
	t.Run("accepted today 0h", func(t *testing.T) {
		assert.NoError(t, parse("0h"))
	})
}

func TestOpenCostMetricTargetType(t *testing.T) {
	tests := []struct {
		name       string
		metricType v2.MetricTargetType
		wantType   v2.MetricTargetType
	}{
		// TODO: the default metricType is planned to change from AverageValue to Value.
		{"default", "", v2.AverageValueMetricType},
		{"AverageValue", v2.AverageValueMetricType, v2.AverageValueMetricType},
		{"Value", v2.ValueMetricType, v2.ValueMetricType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scaler, err := NewOpenCostScaler(&scalersconfig.ScalerConfig{
				TriggerMetadata: openCostMetadata(map[string]string{"costThreshold": "0.5"}),
				MetricType:      tc.metricType,
			})
			require.NoError(t, err)

			specs := scaler.GetMetricSpecForScaling(context.Background())
			require.Len(t, specs, 1)
			require.NotNil(t, specs[0].External)
			target := specs[0].External.Target
			assert.Equal(t, tc.wantType, target.Type)
			if tc.wantType == v2.ValueMetricType {
				require.NotNil(t, target.Value)
				assert.Equal(t, int64(500), target.Value.MilliValue())
			} else {
				require.NotNil(t, target.AverageValue)
				assert.Equal(t, int64(500), target.AverageValue.MilliValue())
			}
		})
	}

	t.Run("Utilization is not supported", func(t *testing.T) {
		_, err := NewOpenCostScaler(&scalersconfig.ScalerConfig{
			TriggerMetadata: openCostMetadata(nil),
			MetricType:      v2.UtilizationMetricType,
		})
		assert.Error(t, err)
	})
}

func TestOpenCostRequestParameters(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		wantQuery url.Values
	}{
		{
			name:      "defaults send window and aggregate, no filter",
			overrides: nil,
			// TODO: the default window is planned to be replaced by an explicit start and end.
			wantQuery: url.Values{"window": {"1h"}, "aggregate": {"namespace"}},
		},
		{
			name:      "explicit values",
			overrides: map[string]string{"window": "24h", "aggregate": "pod", "filter": `namespace:"team-a"`},
			wantQuery: url.Values{"window": {"24h"}, "aggregate": {"pod"}, "filter": {`namespace:"team-a"`}},
		},
		{
			name:      "filter with reserved characters round-trips",
			overrides: map[string]string{"filter": `namespace:"team-a","team-b"+label[app]:"a&b=c"`},
			wantQuery: url.Values{"window": {"1h"}, "aggregate": {"namespace"}, "filter": {`namespace:"team-a","team-b"+label[app]:"a&b=c"`}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			type request struct {
				method, path string
				query        url.Values
			}
			var (
				mu       sync.Mutex
				requests []request
			)
			body := readOpenCostFixture(t, "allocation.json")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, request{r.Method, r.URL.Path, r.URL.Query()})
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			}))
			defer server.Close()

			scaler := newOpenCostTestScaler(t, server.URL, tc.overrides)
			_, err := scaler.getCost(context.Background())
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, requests, 1)
			assert.Equal(t, http.MethodGet, requests[0].method)
			assert.Equal(t, "/allocation", requests[0].path)
			assert.Equal(t, tc.wantQuery, requests[0].query)
		})
	}
}

func TestOpenCostGetCostFromFixtures(t *testing.T) {
	for costType, want := range openCostFixtureAllCosts {
		t.Run("allocation.json "+costType, func(t *testing.T) {
			server := newOpenCostServer(t, http.StatusOK, readOpenCostFixture(t, "allocation.json"))
			cost, err := newOpenCostTestScaler(t, server.URL, map[string]string{"costType": costType}).getCost(context.Background())
			require.NoError(t, err)
			assert.InDelta(t, want, cost, 1e-9)
		})
	}

	for costType, want := range openCostFixtureTeamACosts {
		t.Run("allocation_team_a.json "+costType, func(t *testing.T) {
			server := newOpenCostServer(t, http.StatusOK, readOpenCostFixture(t, "allocation_team_a.json"))
			cost, err := newOpenCostTestScaler(t, server.URL, map[string]string{"costType": costType}).getCost(context.Background())
			require.NoError(t, err)
			assert.InDelta(t, want, cost, 1e-9)
		})
	}

	t.Run("costs of every data set in the response are added up", func(t *testing.T) {
		body := `{"code":200,"data":[{"a":{"name":"a","totalCost":1.5}},{"b":{"name":"b","totalCost":2.25},"c":{"name":"c","totalCost":0.25}}]}`
		server := newOpenCostServer(t, http.StatusOK, []byte(body))
		cost, err := newOpenCostTestScaler(t, server.URL, nil).getCost(context.Background())
		require.NoError(t, err)
		assert.InDelta(t, 4.0, cost, 1e-9)
	})

	// TODO: missing data is planned to become an error. Until then both forms of an empty response are cost 0.
	for _, fixture := range []string{"allocation_empty_set.json", "allocation_no_data.json"} {
		t.Run(fixture+" is cost 0", func(t *testing.T) {
			server := newOpenCostServer(t, http.StatusOK, readOpenCostFixture(t, fixture))
			cost, err := newOpenCostTestScaler(t, server.URL, nil).getCost(context.Background())
			require.NoError(t, err)
			assert.InDelta(t, 0.0, cost, 0)
		})
	}
}

func TestOpenCostGetMetricsAndActivityErrors(t *testing.T) {
	closedServer := httptest.NewServer(http.NotFoundHandler())
	closedURL := closedServer.URL
	closedServer.Close()

	tests := []struct {
		name          string
		status        int
		body          []byte
		serverAddress string
		errContains   []string
	}{
		{
			name:        "HTTP 500 with the OpenCost error envelope",
			status:      http.StatusInternalServerError,
			body:        readOpenCostFixture(t, "error_500.json"),
			errContains: []string{"status 500", "Prometheus query failed"},
		},
		{
			name:        "HTTP 404",
			status:      http.StatusNotFound,
			body:        []byte("404 page not found"),
			errContains: []string{"status 404"},
		},
		{
			name:        "HTTP 503 without a body",
			status:      http.StatusServiceUnavailable,
			errContains: []string{"status 503"},
		},
		{
			name:        "HTTP 200 with an error code in the envelope",
			status:      http.StatusOK,
			body:        readOpenCostFixture(t, "error_500.json"),
			errContains: []string{"code 500", "Prometheus query failed"},
		},
		{
			name:        "HTTP 200 without a code in the envelope",
			status:      http.StatusOK,
			body:        []byte(`{"data":[]}`),
			errContains: []string{"code 0"},
		},
		{
			name:        "truncated JSON",
			status:      http.StatusOK,
			body:        []byte(`{"code":200,"data":[`),
			errContains: []string{"error parsing OpenCost response"},
		},
		{
			name:        "not JSON",
			status:      http.StatusOK,
			body:        []byte("<html>bad gateway</html>"),
			errContains: []string{"error parsing OpenCost response"},
		},
		{
			name:        "data is an object instead of a list",
			status:      http.StatusOK,
			body:        []byte(`{"code":200,"data":{"team-a":{"totalCost":1}}}`),
			errContains: []string{"error parsing OpenCost response"},
		},
		{
			name:          "server is not reachable",
			serverAddress: closedURL,
			errContains:   []string{"error making request to OpenCost"},
		},
		{
			name:          "invalid server address",
			serverAddress: "http://[::1",
			errContains:   []string{"invalid server address"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serverAddress := tc.serverAddress
			if serverAddress == "" {
				serverAddress = newOpenCostServer(t, tc.status, tc.body).URL
			}
			scaler := newOpenCostTestScaler(t, serverAddress, nil)

			metrics, isActive, err := scaler.GetMetricsAndActivity(context.Background(), "test-metric")
			require.Error(t, err)
			assert.ErrorContains(t, err, "error getting cost from OpenCost")
			for _, s := range tc.errContains {
				assert.ErrorContains(t, err, s)
			}
			assert.Empty(t, metrics)
			assert.False(t, isActive)
		})
	}
}

func TestOpenCostMetricValues(t *testing.T) {
	zeroCost := `{"code":200,"data":[{"team-a":{"name":"team-a","totalCost":0}}]}`
	negativeCost := `{"code":200,"data":[{"team-a":{"name":"team-a","totalCost":-0.25}}]}`

	tests := []struct {
		name       string
		fixture    string // a file in testdata/opencost, or
		body       string // an inline response when fixture is empty
		overrides  map[string]string
		wantMetric float64
		wantActive bool
	}{
		// Normal mode: the metric is the cost.
		{
			name: "normal, all namespaces", fixture: "allocation.json",
			overrides:  map[string]string{"costThreshold": "0.5"},
			wantMetric: 0.121, wantActive: true,
		},
		{
			name: "normal, team-a only", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.5", "costType": "cpuCost"},
			wantMetric: 0.02, wantActive: true,
		},
		// Inverse mode: the metric is costThreshold^2 / cost.
		{
			name: "inverse, all namespaces", fixture: "allocation.json",
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 0.25 / 0.121, wantActive: true,
		},
		{
			name: "inverse, team-a only", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 0.25 / 0.037, wantActive: true,
		},
		{
			name: "inverse, cost equal to the threshold gives the threshold", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.037", "inverseScaling": "true"},
			wantMetric: 0.037, wantActive: true,
		},
		{
			name: "inverse, cpuCost", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.1", "costType": "cpuCost", "inverseScaling": "true"},
			wantMetric: 0.5, wantActive: true,
		},
		// Activation compares the cost, not the metric, and the cost must be strictly above it.
		{
			name: "activation, cost equal to activationCostThreshold is inactive", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.5", "activationCostThreshold": "0.037"},
			wantMetric: 0.037, wantActive: false,
		},
		{
			name: "activation, cost just above activationCostThreshold is active", fixture: "allocation_team_a.json",
			overrides:  map[string]string{"costThreshold": "0.5", "activationCostThreshold": "0.036"},
			wantMetric: 0.037, wantActive: true,
		},
		{
			name: "activation in inverse mode uses the cost", fixture: "allocation.json",
			overrides:  map[string]string{"costThreshold": "0.5", "activationCostThreshold": "0.1", "inverseScaling": "true"},
			wantMetric: 0.25 / 0.121, wantActive: true,
		},
		// Cost 0 or below in inverse mode reports 2 x costThreshold.
		{
			name: "inverse, cost 0", body: zeroCost,
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 1.0, wantActive: false,
		},
		{
			name: "inverse, negative cost", body: negativeCost,
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 1.0, wantActive: false,
		},
		{
			name: "normal, negative cost is reported as 0", body: negativeCost,
			overrides:  map[string]string{"costThreshold": "0.5"},
			wantMetric: 0, wantActive: false,
		},
		// TODO: missing data is planned to become an error. Until then an empty response is cost 0, which in
		// inverse mode reports 2 x costThreshold. Both empty forms are covered (no allocations, no match).
		{
			name: "empty set, normal", fixture: "allocation_empty_set.json",
			overrides:  map[string]string{"costThreshold": "0.5"},
			wantMetric: 0, wantActive: false,
		},
		{
			name: "empty set, inverse", fixture: "allocation_empty_set.json",
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 1.0, wantActive: false,
		},
		{
			name: "no data, normal", fixture: "allocation_no_data.json",
			overrides:  map[string]string{"costThreshold": "0.5"},
			wantMetric: 0, wantActive: false,
		},
		{
			name: "no data, inverse", fixture: "allocation_no_data.json",
			overrides:  map[string]string{"costThreshold": "0.5", "inverseScaling": "true"},
			wantMetric: 1.0, wantActive: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			if tc.fixture != "" {
				body = readOpenCostFixture(t, tc.fixture)
			}
			server := newOpenCostServer(t, http.StatusOK, body)
			scaler := newOpenCostTestScaler(t, server.URL, tc.overrides)

			metrics, isActive, err := scaler.GetMetricsAndActivity(context.Background(), "test-metric")
			require.NoError(t, err)
			require.Len(t, metrics, 1)
			assert.Equal(t, "test-metric", metrics[0].MetricName)
			// The metric is reported in milli units.
			assert.InDelta(t, tc.wantMetric, float64(metrics[0].Value.MilliValue())/1000.0, 0.001)
			assert.Equal(t, tc.wantActive, isActive)
		})
	}
}

// OpenCost applies filter to the allocations it returns: a matching filter narrows the response and a
// filter that matches nothing returns data [].
func TestOpenCostFilterMatchesNothing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture := "allocation_no_data.json"
		switch r.URL.Query().Get("filter") {
		case "":
			fixture = "allocation.json"
		case `namespace:"team-a"`:
			fixture = "allocation_team_a.json"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readOpenCostFixture(t, fixture))
	}))
	defer server.Close()

	tests := []struct {
		name       string
		filter     string
		wantCost   float64
		wantMetric float64
		wantActive bool
	}{
		{name: "no filter", filter: "", wantCost: 0.121, wantMetric: 0.25 / 0.121, wantActive: true},
		{name: "filter matches team-a", filter: `namespace:"team-a"`, wantCost: 0.037, wantMetric: 0.25 / 0.037, wantActive: true},
		// TODO: missing data is planned to become an error. Until then a filter that matches nothing is
		// cost 0, which in inverse mode reports 2 x costThreshold.
		{name: "filter matches nothing", filter: `namespace:"no-such-namespace"`, wantCost: 0, wantMetric: 1.0, wantActive: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			overrides := map[string]string{"costThreshold": "0.5", "inverseScaling": "true"}
			if tc.filter != "" {
				overrides["filter"] = tc.filter
			}
			scaler := newOpenCostTestScaler(t, server.URL, overrides)

			cost, err := scaler.getCost(context.Background())
			require.NoError(t, err)
			assert.InDelta(t, tc.wantCost, cost, 1e-9)

			metrics, isActive, err := scaler.GetMetricsAndActivity(context.Background(), "test-metric")
			require.NoError(t, err)
			require.Len(t, metrics, 1)
			assert.InDelta(t, tc.wantMetric, float64(metrics[0].Value.MilliValue())/1000.0, 0.001)
			assert.Equal(t, tc.wantActive, isActive)
		})
	}
}
