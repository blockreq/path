package shannon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pokt-network/poktroll/pkg/polylog/polyzero"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/path/gateway"
	"github.com/pokt-network/path/protocol"
	"github.com/pokt-network/path/reputation"
	reputationstorage "github.com/pokt-network/path/reputation/storage"
)

type tieredPoolFixture struct {
	url      string
	score    float64
	cooldown bool
	probed   bool
}

// MemoryStorage.List does not round-trip serialized URL keys correctly. Keep the
// fixture's exact typed keys so the real service loads scores through GetMultiple.
type tieredPoolStorage struct {
	*reputationstorage.MemoryStorage
	keys []reputation.EndpointKey
}

func (s *tieredPoolStorage) List(_ context.Context, serviceID string) ([]reputation.EndpointKey, error) {
	keys := make([]reputation.EndpointKey, 0, len(s.keys))
	for _, key := range s.keys {
		if serviceID == "" || string(key.ServiceID) == serviceID {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// This fixture exercises the full session lookup, including the tier stage that
// runs after the reputation pool-collapse guard. Survivors alone misses that stage.
type tieredPoolScenario struct {
	*selectionScenario
	sessionID string
}

func newTieredPoolScenario(t *testing.T, granularity string, fixtures ...tieredPoolFixture) *tieredPoolScenario {
	t.Helper()
	ctx := context.Background()
	cfg := reputation.Config{
		Enabled: true, InitialScore: 80, MinThreshold: 30,
		KeyGranularity: granularity, StorageType: "memory", RecoveryTimeout: time.Hour,
	}
	cfg.HydrateDefaults()
	store := &tieredPoolStorage{MemoryStorage: reputationstorage.NewMemoryStorage(cfg.RecoveryTimeout)}
	svc := reputation.NewService(cfg, store)
	logger := polyzero.NewLogger()
	s := &tieredPoolScenario{
		selectionScenario: &selectionScenario{
			t: t, ctx: ctx, svc: svc, serviceID: "base", supplierOf: make(map[string]string),
			p: &Protocol{
				logger: logger, reputationService: svc,
				tieredSelector: reputation.NewTieredSelectorWithLogger(logger, reputation.TieredSelectionConfig{
					Enabled: true, Tier1Threshold: 70, Tier2Threshold: 50,
					Probation: reputation.ProbationConfig{Enabled: false},
				}, cfg.MinThreshold),
				gatewayMode: protocol.GatewayModeCentralized,
				gatewayAddr: "pokt1gateway",
				ownedApps:   map[protocol.ServiceID][]string{"base": {"pokt1app"}},
			},
		},
	}
	for i, fixture := range fixtures {
		s.urls = append(s.urls, fixture.url)
		s.supplierOf[fixture.url] = supplierAddrForIndex(i, 0)
		score := reputation.Score{
			Value: fixture.score, LastUpdated: time.Now(), HasHealthCheckProbe: fixture.probed,
		}
		if fixture.cooldown {
			score.CooldownUntil = time.Now().Add(time.Hour)
		}
		// Start loads these exact persisted scores into the real reputation service.
		key := s.endpointKey(fixture.url, sharedtypes.RPCType_JSON_RPC)
		store.keys = append(store.keys, key)
		require.NoError(t, store.Set(ctx, key, score))
	}
	require.NoError(t, svc.Start(ctx))
	t.Cleanup(func() { require.NoError(t, svc.Stop()) })
	for _, fixture := range fixtures {
		score, err := svc.GetScore(ctx, s.endpointKey(fixture.url, sharedtypes.RPCType_JSON_RPC))
		require.NoError(t, err, "fixture score must be loaded for %s", fixture.url)
		require.Equal(t, fixture.score, score.Value, "fixture score changed for %s", fixture.url)
		require.Equal(t, fixture.probed, score.HasHealthCheckProbe, "fixture probe state changed for %s", fixture.url)
		require.Equal(t, fixture.cooldown, score.IsInCooldown(), "fixture cooldown state changed for %s", fixture.url)
	}
	s.rotate(0)
	return s
}

func (s *tieredPoolScenario) session() sessiontypes.Session {
	return sessiontypes.Session{
		SessionId: s.sessionID,
		Header: &sessiontypes.SessionHeader{
			SessionId: s.sessionID, ServiceId: string(s.serviceID), ApplicationAddress: "pokt1app",
		},
		Application: &apptypes.Application{
			Address: "pokt1app", DelegateeGatewayAddresses: []string{"pokt1gateway"},
		},
	}
}

func (s *tieredPoolScenario) rotate(generation int) {
	s.t.Helper()
	s.RotateSuppliers(generation)
	s.sessionID = fmt.Sprintf("tiered-pool-session-%d", generation)
	s.p.sessionEndpointsCache.Store(s.sessionID, s.endpointMap())
	s.p.FullNode = &healthCheckFullNode{session: s.session()}
}

func (s *tieredPoolScenario) addr(url string) protocol.EndpointAddr {
	return protocol.EndpointAddr(s.supplierOf[url] + "-" + url)
}

func (s *tieredPoolScenario) assertCallers(want ...string) {
	s.t.Helper()
	wantAddrs := make([]protocol.EndpointAddr, 0, len(want))
	for _, url := range want {
		wantAddrs = append(wantAddrs, s.addr(url))
	}
	for _, caller := range []struct {
		name string
		call func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error)
	}{
		{"session", func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
			return s.p.getSessionsUniqueEndpoints(s.ctx, s.serviceID, []sessiontypes.Session{s.session()}, true, sharedtypes.RPCType_JSON_RPC, nil, "")
		}},
		{"unique", func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
			return s.p.getUniqueEndpoints(s.ctx, s.serviceID, []sessiontypes.Session{s.session()}, true, sharedtypes.RPCType_JSON_RPC, nil, "")
		}},
	} {
		got, rpcType, err := caller.call()
		require.NoError(s.t, err, "%s endpoint lookup must retain the expected current subset", caller.name)
		require.Equal(s.t, sharedtypes.RPCType_JSON_RPC, rpcType)
		gotAddrs := make([]protocol.EndpointAddr, 0, len(got))
		for addr := range got {
			gotAddrs = append(gotAddrs, addr)
		}
		require.ElementsMatch(s.t, wantAddrs, gotAddrs, "%s lookup returned the wrong endpoint pool", caller.name)
	}
	available, _, err := s.p.AvailableHTTPEndpoints(s.ctx, s.serviceID, sharedtypes.RPCType_JSON_RPC, nil)
	require.NoError(s.t, err)
	require.ElementsMatch(s.t, wantAddrs, available, "the gateway must receive the same filtered pool")
}

func (s *tieredPoolScenario) assertContext(url string) {
	s.t.Helper()
	rc, _, err := s.p.BuildHTTPRequestContextForEndpoint(s.ctx, s.serviceID, s.addr(url), sharedtypes.RPCType_JSON_RPC, nil, true)
	require.NoError(s.t, err, "a selected endpoint must remain available when building its request context")
	require.Equal(s.t, s.addr(url), rc.(*requestContext).selectedEndpoint.Addr())
}

func TestTieredPoolCollapse_LeastBadThroughProductionCallers(t *testing.T) {
	urls := []string{"https://a.op-alpha.example", "https://a.op-beta.example", "https://b.op-alpha.example"}
	for _, granularity := range []string{reputation.KeyGranularityURL, reputation.KeyGranularityDomain} {
		for _, state := range []struct {
			name     string
			scores   []float64
			cooldown bool
		}{
			{"all below minimum", []float64{29, 10, 29}, false},
			{"all below minimum and in cooldown", []float64{29, 10, 29}, true},
			{"all in cooldown above minimum", []float64{69, 40, 69}, true},
		} {
			t.Run(granularity+"/"+state.name, func(t *testing.T) {
				s := newTieredPoolScenario(t, granularity,
					tieredPoolFixture{url: urls[0], score: state.scores[0], cooldown: state.cooldown},
					tieredPoolFixture{url: urls[1], score: state.scores[1], cooldown: state.cooldown},
					tieredPoolFixture{url: urls[2], score: state.scores[2], cooldown: state.cooldown})
				fallback := &harnessEndpoint{supplier: "fallback", url: "https://fallback.example"}
				s.p.serviceFallbackMap = map[protocol.ServiceID]serviceFallback{
					s.serviceID: {Endpoints: map[protocol.EndpointAddr]endpoint{fallback.Addr(): fallback}},
				}
				for generation := range 2 {
					s.rotate(generation)
					s.assertCallers(urls[0], urls[2])
					s.assertContext(urls[0])
					s.assertContext(urls[2])
				}
			})
		}
	}
}

func TestTieredPoolCollapse_HealthyTiersUnchanged(t *testing.T) {
	urls := []string{"https://a.op-alpha.example", "https://a.op-beta.example", "https://a.op-gamma.example"}
	for _, tc := range []struct {
		name   string
		scores []float64
		want   []string
	}{
		{"tier one boundary", []float64{70, 50, 30}, urls[:1]},
		{"tier two keeps its full band", []float64{69, 50, 30}, urls[:2]},
		{"tier three keeps its full band", []float64{49, 30, 10}, urls[:2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTieredPoolScenario(t, reputation.KeyGranularityURL,
				tieredPoolFixture{url: urls[0], score: tc.scores[0]},
				tieredPoolFixture{url: urls[1], score: tc.scores[1]},
				tieredPoolFixture{url: urls[2], score: tc.scores[2]})
			s.assertCallers(tc.want...)
		})
	}
}

func TestTieredPoolCollapse_GlobalMaximumAcrossSessions(t *testing.T) {
	const best = "https://a.op-alpha.example"
	const lower = "https://a.op-beta.example"
	const otherSessionBest = "https://a.op-gamma.example"
	s := newTieredPoolScenario(t, reputation.KeyGranularityURL,
		tieredPoolFixture{url: best, score: 29},
		tieredPoolFixture{url: lower, score: 10},
		tieredPoolFixture{url: otherSessionBest, score: 19})
	all := s.endpointMap()
	s.p.sessionEndpointsCache.Store(s.sessionID, map[protocol.EndpointAddr]endpoint{s.addr(best): all[s.addr(best)]})
	other := s.session()
	other.SessionId, other.Header.SessionId = "other-session", "other-session"
	other.Header.ApplicationAddress, other.Application.Address = "pokt1otherapp", "pokt1otherapp"
	s.p.sessionEndpointsCache.Store(other.SessionId, map[protocol.EndpointAddr]endpoint{
		s.addr(lower): all[s.addr(lower)], s.addr(otherSessionBest): all[s.addr(otherSessionBest)],
	})
	sessions := []sessiontypes.Session{s.session(), other}
	// Each session independently keeps its maximum (29 and 19). The final tier
	// stage must retain only the global maximum, rather than restoring both.
	for _, call := range []func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error){
		func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
			return s.p.getSessionsUniqueEndpoints(s.ctx, s.serviceID, sessions, true, sharedtypes.RPCType_JSON_RPC, nil, "")
		},
		func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
			return s.p.getUniqueEndpoints(s.ctx, s.serviceID, sessions, true, sharedtypes.RPCType_JSON_RPC, nil, "")
		},
	} {
		got, _, err := call()
		require.NoError(t, err)
		require.Len(t, got, 1, "the lower maximum from another session must not dilute the least-bad pool")
		require.Contains(t, got, s.addr(best))
	}
}

type tieredPoolUnsupportedRPC struct{ *harnessEndpoint }

func (e *tieredPoolUnsupportedRPC) GetURL(rpcType sharedtypes.RPCType) string {
	if rpcType == sharedtypes.RPCType_JSON_RPC {
		return ""
	}
	return e.url
}

func TestTieredPoolCollapse_DoesNotResurrectExcludedEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name          string
		excludedURL   string
		configure     func(*tieredPoolScenario, string)
		blocksRequest bool
	}{
		{"blocked supplier", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, url string) {
			s.p.unifiedServicesConfig = &gateway.UnifiedServicesConfig{Services: []gateway.ServiceConfig{{ID: s.serviceID, BlockedSuppliers: []string{s.supplierOf[url]}}}}
		}, true},
		{"blocked domain", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, _ string) {
			s.p.blockedDomains = compileBlocklist(s.t, gateway.BlockedDomainConfig{Domain: "op-gamma.example"})
		}, true},
		{"HTTPS policy", "http://excluded.op-gamma.example", func(s *tieredPoolScenario, _ string) {
			s.p.endpointPolicy = gateway.EndpointPolicyConfig{RequireHTTPS: true}
		}, true},
		{"domain policy", "https://127.0.0.1:8545", func(s *tieredPoolScenario, _ string) {
			s.p.endpointPolicy = gateway.EndpointPolicyConfig{RequireDomain: true}
		}, true},
		{"unsupported RPC type", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, url string) {
			eps := s.endpointMap()
			eps[s.addr(url)] = &tieredPoolUnsupportedRPC{harnessEndpoint: &harnessEndpoint{supplier: s.supplierOf[url], url: url}}
			s.p.sessionEndpointsCache.Store(s.sessionID, eps)
		}, false},
		{"unprobed endpoint", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, _ string) {
			s.p.preferProbed = true
		}, false},
		{"blacklisted supplier", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, url string) {
			s.p.supplierBlacklist = newSupplierBlacklist()
			s.p.supplierBlacklist.Blacklist(s.serviceID, s.supplierOf[url], "invalid signature")
		}, false},
		{"drained domain", "https://excluded.op-gamma.example", func(s *tieredPoolScenario, _ string) {
			s.Drain("op-gamma.example", sharedtypes.RPCType_JSON_RPC, time.Hour)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const survivor = "https://a.op-alpha.example"
			s := newTieredPoolScenario(t, reputation.KeyGranularityURL,
				tieredPoolFixture{url: survivor, score: 29, probed: true},
				tieredPoolFixture{url: "https://a.op-beta.example", score: 10},
				tieredPoolFixture{url: tc.excludedURL, score: 29})
			// A healthy score absent from the current session must not become a
			// candidate when current endpoints have no qualifying tier.
			staleKey := s.svc.KeyBuilderForService(s.serviceID).BuildKey(s.serviceID, "oldsupplier-https://stale.example", sharedtypes.RPCType_JSON_RPC)
			require.NoError(t, s.svc.ResetScore(s.ctx, staleKey))
			tc.configure(s, tc.excludedURL)
			s.assertCallers(survivor)
			if tc.blocksRequest {
				_, _, err := s.p.BuildHTTPRequestContextForEndpoint(s.ctx, s.serviceID, s.addr(tc.excludedURL), sharedtypes.RPCType_JSON_RPC, nil, true)
				require.ErrorIs(t, err, protocol.ErrEndpointUnavailable, "a requested address cannot restore an endpoint removed by an earlier filter")
			}
		})
	}
}

func TestTieredPoolCollapse_HardExclusionCanEmptyPool(t *testing.T) {
	const blocked = "https://a.op-alpha.example"
	s := newTieredPoolScenario(t, reputation.KeyGranularityURL, tieredPoolFixture{url: blocked, score: 29})
	s.p.blockedDomains = compileBlocklist(t, gateway.BlockedDomainConfig{Domain: "op-alpha.example"})
	_, _, err := s.p.getSessionsUniqueEndpoints(s.ctx, s.serviceID, []sessiontypes.Session{s.session()}, true, sharedtypes.RPCType_JSON_RPC, nil, "")
	require.ErrorIs(t, err, errProtocolContextSetupNoEndpoints)
	_, _, err = s.p.getUniqueEndpoints(s.ctx, s.serviceID, []sessiontypes.Session{s.session()}, true, sharedtypes.RPCType_JSON_RPC, nil, "")
	require.ErrorIs(t, err, errProtocolContextSetupNoEndpoints)
}

func TestTieredPoolCollapse_SelectedEndpointScoreChange(t *testing.T) {
	const selected = "https://a.op-alpha.example"
	const peer = "https://a.op-gamma.example"
	for _, tc := range []struct {
		name  string
		score float64
	}{
		{"degraded selected endpoint", 29},
		{"healthy selected endpoint drops to tier two", 70},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTieredPoolScenario(t, reputation.KeyGranularityURL,
				tieredPoolFixture{url: selected, score: tc.score},
				tieredPoolFixture{url: peer, score: tc.score})
			available, _, err := s.p.AvailableHTTPEndpoints(s.ctx, s.serviceID, sharedtypes.RPCType_JSON_RPC, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, []protocol.EndpointAddr{s.addr(selected), s.addr(peer)}, available)
			require.NoError(t, s.svc.RecordSignal(s.ctx, s.endpointKey(selected, sharedtypes.RPCType_JSON_RPC), reputation.NewMajorErrorSignal("score changed after selection", 0)))
			s.assertContext(selected)
			s.assertCallers(peer)
		})
	}
}

type tieredPoolMultiSessionFullNode struct {
	*healthCheckFullNode
	other sessiontypes.Session
}

func (f *tieredPoolMultiSessionFullNode) GetSession(ctx context.Context, serviceID protocol.ServiceID, appAddr string) (sessiontypes.Session, error) {
	if appAddr == f.other.Application.Address {
		return f.other, nil
	}
	return f.healthCheckFullNode.GetSession(ctx, serviceID, appAddr)
}

func (f *tieredPoolMultiSessionFullNode) GetSessionWithExtendedValidity(ctx context.Context, serviceID protocol.ServiceID, appAddr string) (sessiontypes.Session, error) {
	return f.GetSession(ctx, serviceID, appAddr)
}

func TestTieredPoolCollapse_SelectedEndpointScoreChangeAcrossSessions(t *testing.T) {
	const selected = "https://a.op-alpha.example"
	const peer = "https://a.op-gamma.example"
	for _, tc := range []struct {
		name          string
		score         float64
		retainRequest bool
	}{
		{"below minimum selected endpoint loses to another session maximum", 29, false},
		{"selected endpoint above minimum keeps existing escape", 70, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTieredPoolScenario(t, reputation.KeyGranularityURL,
				tieredPoolFixture{url: selected, score: tc.score},
				tieredPoolFixture{url: "https://a.op-beta.example", score: 10},
				tieredPoolFixture{url: peer, score: tc.score})
			all := s.endpointMap()
			s.p.sessionEndpointsCache.Store(s.sessionID, map[protocol.EndpointAddr]endpoint{s.addr(selected): all[s.addr(selected)]})
			other := s.session()
			other.SessionId, other.Header.SessionId = "other-session", "other-session"
			other.Header.ApplicationAddress, other.Application.Address = "pokt1otherapp", "pokt1otherapp"
			s.p.sessionEndpointsCache.Store(other.SessionId, map[protocol.EndpointAddr]endpoint{
				s.addr(s.urls[1]): all[s.addr(s.urls[1])], s.addr(peer): all[s.addr(peer)],
			})
			s.p.FullNode = &tieredPoolMultiSessionFullNode{healthCheckFullNode: &healthCheckFullNode{session: s.session()}, other: other}
			s.p.ownedApps[s.serviceID] = []string{"pokt1app", "pokt1otherapp"}
			available, _, err := s.p.AvailableHTTPEndpoints(s.ctx, s.serviceID, sharedtypes.RPCType_JSON_RPC, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, []protocol.EndpointAddr{s.addr(selected), s.addr(peer)}, available)
			require.NoError(t, s.svc.RecordSignal(s.ctx, s.endpointKey(selected, sharedtypes.RPCType_JSON_RPC), reputation.NewMajorErrorSignal("score changed after selection", 0)))

			// Both sessions contribute a current candidate after the score change.
			// A selected score 19 must not override the other session's maximum 29;
			// the existing requested-endpoint escape still applies to score 60.
			want := []protocol.EndpointAddr{s.addr(peer)}
			if tc.retainRequest {
				want = append(want, s.addr(selected))
			}
			sessions := []sessiontypes.Session{s.session(), other}
			for _, call := range []func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error){
				func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
					return s.p.getSessionsUniqueEndpoints(s.ctx, s.serviceID, sessions, true, sharedtypes.RPCType_JSON_RPC, nil, s.addr(selected))
				},
				func() (map[protocol.EndpointAddr]endpoint, sharedtypes.RPCType, error) {
					return s.p.getUniqueEndpoints(s.ctx, s.serviceID, sessions, true, sharedtypes.RPCType_JSON_RPC, nil, s.addr(selected))
				},
			} {
				got, _, err := call()
				require.NoError(t, err)
				gotAddrs := make([]protocol.EndpointAddr, 0, len(got))
				for addr := range got {
					gotAddrs = append(gotAddrs, addr)
				}
				require.ElementsMatch(t, want, gotAddrs, "the requested endpoint must obey the existing minimum-threshold contract")
			}
			if tc.retainRequest {
				s.assertContext(selected)
			} else {
				_, _, err := s.p.BuildHTTPRequestContextForEndpoint(s.ctx, s.serviceID, s.addr(selected), sharedtypes.RPCType_JSON_RPC, nil, true)
				require.ErrorIs(t, err, protocol.ErrEndpointUnavailable)
			}
		})
	}
}
