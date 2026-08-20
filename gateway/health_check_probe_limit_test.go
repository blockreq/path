package gateway

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/poktroll/pkg/polylog/polyzero"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/path/protocol"
	"github.com/pokt-network/path/reputation"
	reputationstorage "github.com/pokt-network/path/reputation/storage"
)

// numberedEndpoints returns n distinct-URL session endpoints, Addr-sorted by
// construction (supplier-%02d). SessionIDs are unique so a counting protocol
// can tell which endpoints a cycle actually scheduled.
func numberedEndpoints(n int) []EndpointInfo {
	out := make([]EndpointInfo, n)
	for i := 0; i < n; i++ {
		url := fmt.Sprintf("https://e%02d.example.com", i)
		out[i] = EndpointInfo{
			Addr:      protocol.EndpointAddr(fmt.Sprintf("pokt1supplier%02d-%s", i, url)),
			HTTPURL:   url,
			SessionID: fmt.Sprintf("sess-%02d", i),
		}
	}
	return out
}

func addrsOf(eps []EndpointInfo) []string {
	out := make([]string, len(eps))
	for i, ep := range eps {
		out[i] = string(ep.Addr)
	}
	sort.Strings(out)
	return out
}

func newMemoryReputation(t *testing.T) reputation.ReputationService {
	t.Helper()
	cfg := reputation.Config{Enabled: true, InitialScore: 80, MinThreshold: 30, StorageType: "memory"}
	cfg.HydrateDefaults()
	store := reputationstorage.NewMemoryStorage(cfg.RecoveryTimeout)
	svc := reputation.NewService(cfg, store)
	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })
	return svc
}

func recordJSONRPCScore(t *testing.T, svc reputation.ReputationService, serviceID protocol.ServiceID, addr protocol.EndpointAddr, sig reputation.Signal) {
	t.Helper()
	key := svc.KeyBuilderForService(serviceID).BuildKey(serviceID, addr, sharedtypes.RPCType_JSON_RPC)
	require.NoError(t, svc.RecordSignal(context.Background(), key, sig))
}

func TestHydrateDefaults_ProbeKnobsStayZero(t *testing.T) {
	cfg := &ActiveHealthChecksConfig{}
	cfg.HydrateDefaults(true)
	require.Equal(t, 0, cfg.MaxProbeEndpoints, "zero-value must keep probe-all")
	require.False(t, cfg.PreferProbed, "zero-value must keep today's selection")
	require.Equal(t, 0, cfg.RecoveryProbes, "zero-value must add no recovery probes")

	// Explicit values must survive hydration.
	cfg2 := &ActiveHealthChecksConfig{MaxProbeEndpoints: 2, PreferProbed: true, RecoveryProbes: 1}
	cfg2.HydrateDefaults(true)
	require.Equal(t, 2, cfg2.MaxProbeEndpoints)
	require.True(t, cfg2.PreferProbed)
	require.Equal(t, 1, cfg2.RecoveryProbes)
}

func TestValidate_ProbeKnobsRejectNegative(t *testing.T) {
	require.NoError(t, (&ActiveHealthChecksConfig{}).Validate())
	require.NoError(t, (&ActiveHealthChecksConfig{MaxProbeEndpoints: 2, RecoveryProbes: 1, PreferProbed: true}).Validate())

	err := (&ActiveHealthChecksConfig{MaxProbeEndpoints: -1}).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_probe_endpoints")

	err = (&ActiveHealthChecksConfig{RecoveryProbes: -1}).Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "recovery_probes")
}

func Test_selectProbeEndpoints_MaxZeroProbesAll(t *testing.T) {
	eps := numberedEndpoints(10)
	executor := &HealthCheckExecutor{
		config: &ActiveHealthChecksConfig{MaxProbeEndpoints: 0},
		logger: polyzero.NewLogger(),
	}
	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Equal(t, addrsOf(eps), addrsOf(got), "max_probe_endpoints=0 must still schedule every endpoint")
}

func Test_selectProbeEndpoints_MaxTwoOfTen(t *testing.T) {
	eps := numberedEndpoints(10)
	executor := &HealthCheckExecutor{
		config:        &ActiveHealthChecksConfig{MaxProbeEndpoints: 2},
		logger:        polyzero.NewLogger(),
		reputationSvc: newMemoryReputation(t),
	}
	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Len(t, got, 2, "max_probe_endpoints=2 with 10 endpoints must probe exactly 2")
	// All scores are initial_score, so ranking is by Addr. The two lowest addrs win.
	require.Equal(t, addrsOf(eps[:2]), addrsOf(got))
}

func Test_selectProbeEndpoints_RecoveryAddsOne(t *testing.T) {
	eps := numberedEndpoints(10)
	executor := &HealthCheckExecutor{
		config: &ActiveHealthChecksConfig{
			MaxProbeEndpoints: 2,
			RecoveryProbes:    1,
		},
		logger:        polyzero.NewLogger(),
		reputationSvc: newMemoryReputation(t),
	}
	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Len(t, got, 3, "max=2 + recovery_probes=1 must yield 3 unique endpoints")

	seen := map[string]struct{}{}
	for _, ep := range got {
		_, dup := seen[string(ep.Addr)]
		require.False(t, dup, "recovery probe must be distinct from the top-N set")
		seen[string(ep.Addr)] = struct{}{}
	}
}

func Test_selectProbeEndpoints_RanksByJSONRPCReputation(t *testing.T) {
	eps := numberedEndpoints(10)
	svc := newMemoryReputation(t)
	executor := &HealthCheckExecutor{
		config:        &ActiveHealthChecksConfig{MaxProbeEndpoints: 2},
		logger:        polyzero.NewLogger(),
		reputationSvc: svc,
	}

	// Raise endpoints 7 and 3 well above initial_score. Top-2 must be those two,
	// not the Addr-sorted first pair.
	for i := 0; i < 4; i++ {
		recordJSONRPCScore(t, svc, "eth", eps[7].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
	}
	for i := 0; i < 2; i++ {
		recordJSONRPCScore(t, svc, "eth", eps[3].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
	}

	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Equal(t, []string{string(eps[3].Addr), string(eps[7].Addr)}, addrsOf(got))
}

func Test_selectProbeEndpoints_JSONRPCScoreIgnoresWebsocket(t *testing.T) {
	eps := numberedEndpoints(5)
	svc := newMemoryReputation(t)
	executor := &HealthCheckExecutor{
		config:        &ActiveHealthChecksConfig{MaxProbeEndpoints: 1},
		logger:        polyzero.NewLogger(),
		reputationSvc: svc,
	}

	// A websocket-only boost on endpoint 4 must not make it win the json_rpc ranking.
	wsKey := svc.KeyBuilderForService("eth").BuildKey("eth", eps[4].Addr, sharedtypes.RPCType_WEBSOCKET)
	for i := 0; i < 4; i++ {
		require.NoError(t, svc.RecordSignal(context.Background(), wsKey, reputation.NewRecoverySuccessSignal(10*time.Millisecond)))
	}
	recordJSONRPCScore(t, svc, "eth", eps[1].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))

	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Len(t, got, 1)
	require.Equal(t, eps[1].Addr, got[0].Addr, "probe ranking must use the json_rpc score, not websocket")
}

func Test_selectProbeEndpoints_RecoveryPrefersLowScore(t *testing.T) {
	eps := numberedEndpoints(6)
	svc := newMemoryReputation(t)
	executor := &HealthCheckExecutor{
		config: &ActiveHealthChecksConfig{
			MaxProbeEndpoints: 2,
			RecoveryProbes:    1,
		},
		logger:        polyzero.NewLogger(),
		reputationSvc: svc,
	}

	// Push 0 and 1 to the top. Penalize 5 the most of the remainder so recovery picks it.
	for i := 0; i < 4; i++ {
		recordJSONRPCScore(t, svc, "eth", eps[0].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
		recordJSONRPCScore(t, svc, "eth", eps[1].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
	}
	recordJSONRPCScore(t, svc, "eth", eps[5].Addr, reputation.NewCriticalErrorSignal("lag", 20*time.Millisecond))
	recordJSONRPCScore(t, svc, "eth", eps[5].Addr, reputation.NewCriticalErrorSignal("lag", 20*time.Millisecond))

	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Len(t, got, 3)
	require.Contains(t, addrsOf(got), string(eps[0].Addr))
	require.Contains(t, addrsOf(got), string(eps[1].Addr))
	require.Contains(t, addrsOf(got), string(eps[5].Addr), "recovery_probes must pick the lowest-score remainder endpoint")
}

func Test_selectProbeEndpoints_MissingScoreIsInitial(t *testing.T) {
	eps := numberedEndpoints(4)
	svc := newMemoryReputation(t)
	executor := &HealthCheckExecutor{
		config:        &ActiveHealthChecksConfig{MaxProbeEndpoints: 2},
		logger:        polyzero.NewLogger(),
		reputationSvc: svc,
	}

	// Only endpoint 3 has a score, and it is BELOW initial. The two un-scored
	// endpoints at initial_score 80 must fill the top-N so a fresh session
	// still gets probed.
	recordJSONRPCScore(t, svc, "eth", eps[3].Addr, reputation.NewCriticalErrorSignal("lag", 20*time.Millisecond))

	got := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.NotContains(t, addrsOf(got), string(eps[3].Addr))
	require.Equal(t, addrsOf(eps[:2]), addrsOf(got))
}

func Test_selectProbeEndpoints_BackendDedupCollapsesReducedSet(t *testing.T) {
	// Two highest-score endpoints share a backend URL; a third unique URL is
	// lower. max=2 selects the pair, then groupEndpointsByURL must still
	// collapse them to one relay.
	shared := "https://shared.example.com"
	eps := []EndpointInfo{
		{Addr: "pokt1a-https://shared.example.com", HTTPURL: shared, SessionID: "s-a"},
		{Addr: "pokt1b-https://shared.example.com", HTTPURL: shared, SessionID: "s-b"},
		{Addr: "pokt1c-https://other.example.com", HTTPURL: "https://other.example.com", SessionID: "s-c"},
		{Addr: "pokt1d-https://third.example.com", HTTPURL: "https://third.example.com", SessionID: "s-d"},
	}
	svc := newMemoryReputation(t)
	for i := 0; i < 4; i++ {
		recordJSONRPCScore(t, svc, "eth", eps[0].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
		recordJSONRPCScore(t, svc, "eth", eps[1].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
	}

	executor := &HealthCheckExecutor{
		config:        &ActiveHealthChecksConfig{MaxProbeEndpoints: 2},
		logger:        polyzero.NewLogger(),
		reputationSvc: svc,
	}
	reduced := executor.selectProbeEndpoints(context.Background(), "eth", eps)
	require.Len(t, reduced, 2)
	require.Equal(t, []string{string(eps[0].Addr), string(eps[1].Addr)}, addrsOf(reduced))

	groups := groupEndpointsByURL(reduced)
	require.Len(t, groups, 1, "backend-URL dedup must still collapse the reduced set")
	require.Len(t, groups[0], 2)
}

// probeLimitProtocol records the session IDs a cycle actually scheduled, without
// firing a relay (IsSessionActive returns false so runEndpointChecks never runs).
type probeLimitProtocol struct {
	*mockProtocolForRetry
	mu       sync.Mutex
	sessions []string
}

func (m *probeLimitProtocol) IsSessionActive(_ context.Context, _ protocol.ServiceID, sessionID string) bool {
	m.mu.Lock()
	m.sessions = append(m.sessions, sessionID)
	m.mu.Unlock()
	return false
}

func (m *probeLimitProtocol) uniqueSessions() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]struct{}{}
	for _, s := range m.sessions {
		seen[s] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func newProbeLimitExecutor(t *testing.T, cfg *ActiveHealthChecksConfig) (*HealthCheckExecutor, *probeLimitProtocol) {
	t.Helper()
	proto := &probeLimitProtocol{mockProtocolForRetry: &mockProtocolForRetry{}}
	executor := NewHealthCheckExecutor(HealthCheckExecutorConfig{
		Config:     cfg,
		Logger:     polyzero.NewLogger(),
		Protocol:   proto,
		MaxWorkers: 8,
	})
	t.Cleanup(executor.Stop)
	return executor, proto
}

func Test_RunAllChecksViaProtocol_MaxZeroStillSchedulesAll(t *testing.T) {
	fls := false
	cfg := &ActiveHealthChecksConfig{
		Enabled:           true,
		MaxProbeEndpoints: 0,
		BackendDedup:      &fls, // one job per endpoint, so unique sessions == scheduled endpoints
		Local: []ServiceHealthCheckConfig{{
			ServiceID: "eth",
			Checks:    []HealthCheckConfig{{Name: "probe", Type: HealthCheckTypeJSONRPC, Method: http.MethodPost, Path: "/"}},
		}},
	}
	executor, proto := newProbeLimitExecutor(t, cfg)
	eps := numberedEndpoints(10)

	require.NoError(t, executor.RunAllChecksViaProtocol(context.Background(), func(protocol.ServiceID) ([]EndpointInfo, error) {
		return eps, nil
	}))
	require.Equal(t, 10, len(proto.uniqueSessions()), "max_probe_endpoints=0 must still schedule all 10 endpoints")
}

func Test_RunAllChecksViaProtocol_MaxTwoLimitsRelays(t *testing.T) {
	fls := false
	cfg := &ActiveHealthChecksConfig{
		Enabled:           true,
		MaxProbeEndpoints: 2,
		BackendDedup:      &fls,
		Local: []ServiceHealthCheckConfig{{
			ServiceID: "eth",
			Checks:    []HealthCheckConfig{{Name: "probe", Type: HealthCheckTypeJSONRPC, Method: http.MethodPost, Path: "/"}},
		}},
	}
	executor, proto := newProbeLimitExecutor(t, cfg)
	eps := numberedEndpoints(10)

	require.NoError(t, executor.RunAllChecksViaProtocol(context.Background(), func(protocol.ServiceID) ([]EndpointInfo, error) {
		return eps, nil
	}))
	require.Equal(t, 2, len(proto.uniqueSessions()), "max_probe_endpoints=2 must schedule only 2 HC relays")
}

func Test_RunAllChecksViaProtocol_RecoveryProbesAddsThirdRelay(t *testing.T) {
	fls := false
	cfg := &ActiveHealthChecksConfig{
		Enabled:           true,
		MaxProbeEndpoints: 2,
		RecoveryProbes:    1,
		BackendDedup:      &fls,
		Local: []ServiceHealthCheckConfig{{
			ServiceID: "eth",
			Checks:    []HealthCheckConfig{{Name: "probe", Type: HealthCheckTypeJSONRPC, Method: http.MethodPost, Path: "/"}},
		}},
	}
	executor, proto := newProbeLimitExecutor(t, cfg)
	eps := numberedEndpoints(10)

	require.NoError(t, executor.RunAllChecksViaProtocol(context.Background(), func(protocol.ServiceID) ([]EndpointInfo, error) {
		return eps, nil
	}))
	require.Equal(t, 3, len(proto.uniqueSessions()), "max=2 + recovery_probes=1 must schedule 3 unique endpoints")
}

func Test_RunAllChecksViaProtocol_BackendDedupOnReducedSet(t *testing.T) {
	// Default backend dedup stays ON. Two of the top-N share a URL, so the
	// reduced set of 2 must collapse to 1 HTTP job (one unique session on the
	// representative; the sibling is fanned, not scheduled as its own job).
	cfg := &ActiveHealthChecksConfig{
		Enabled:           true,
		MaxProbeEndpoints: 2,
		Local: []ServiceHealthCheckConfig{{
			ServiceID: "eth",
			Checks:    []HealthCheckConfig{{Name: "probe", Type: HealthCheckTypeJSONRPC, Method: http.MethodPost, Path: "/"}},
		}},
	}
	svc := newMemoryReputation(t)
	shared := "https://shared.example.com"
	eps := []EndpointInfo{
		{Addr: "pokt1a-https://shared.example.com", HTTPURL: shared, SessionID: "s-a"},
		{Addr: "pokt1b-https://shared.example.com", HTTPURL: shared, SessionID: "s-b"},
		{Addr: "pokt1c-https://other.example.com", HTTPURL: "https://other.example.com", SessionID: "s-c"},
	}
	for i := 0; i < 4; i++ {
		recordJSONRPCScore(t, svc, "eth", eps[0].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
		recordJSONRPCScore(t, svc, "eth", eps[1].Addr, reputation.NewRecoverySuccessSignal(10*time.Millisecond))
	}

	proto := &probeLimitProtocol{mockProtocolForRetry: &mockProtocolForRetry{}}
	executor := NewHealthCheckExecutor(HealthCheckExecutorConfig{
		Config:        cfg,
		Logger:        polyzero.NewLogger(),
		Protocol:      proto,
		ReputationSvc: svc,
		MaxWorkers:    8,
	})
	t.Cleanup(executor.Stop)

	require.NoError(t, executor.RunAllChecksViaProtocol(context.Background(), func(protocol.ServiceID) ([]EndpointInfo, error) {
		return eps, nil
	}))

	// One URL group → one submitted job. The job walks the group looking for an
	// active session (we return false), so both siblings may appear, but there
	// is still a single job / a single backend URL being probed.
	require.ElementsMatch(t, []string{"s-a", "s-b"}, proto.uniqueSessions())
	require.NotContains(t, proto.uniqueSessions(), "s-c", "the unique-URL endpoint outside the reduced set must not be scheduled")
}
