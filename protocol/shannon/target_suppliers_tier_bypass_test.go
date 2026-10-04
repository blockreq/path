package shannon

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/poktroll/pkg/polylog/polyzero"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/path/protocol"
	"github.com/pokt-network/path/reputation"
	reputationstorage "github.com/pokt-network/path/reputation/storage"
)

// TestFilterToHighestTier_KeepsLeastBadWhenAllBelowThreshold ensures that ordinary
// tier selection preserves the pool-collapse guard's least-bad candidates. Supplier
// pins still bypass tier selection separately, including when healthier peers exist.
func TestFilterToHighestTier_KeepsLeastBadWhenAllBelowThreshold(t *testing.T) {
	ctx := context.Background()
	logger := polyzero.NewLogger()
	serviceID := protocol.ServiceID("mantle")
	rpcType := sharedtypes.RPCType_JSON_RPC

	repConfig := reputation.Config{
		Enabled:        true,
		InitialScore:   80,
		MinThreshold:   20,
		KeyGranularity: "per-endpoint",
	}
	repSvc := reputation.NewService(repConfig, reputationstorage.NewMemoryStorage(10*time.Minute))

	// Probation disabled so the normal (non-probation) tier path is exercised.
	tieredConfig := reputation.TieredSelectionConfig{
		Enabled:        true,
		Tier1Threshold: 80,
		Tier2Threshold: 50,
		Probation:      reputation.ProbationConfig{Enabled: false},
	}
	p := &Protocol{
		reputationService: repSvc,
		tieredSelector:    reputation.NewTieredSelectorWithLogger(logger, tieredConfig, repConfig.MinThreshold),
	}

	addrs := []protocol.EndpointAddr{
		"pokt1a-https://dopokt1.relayminer.example.net",
		"pokt1b-https://nr.relayminer.example.net",
	}
	endpoints := make(map[protocol.EndpointAddr]endpoint, len(addrs))
	for _, addr := range addrs {
		endpoints[addr] = &mockEndpoint{addr: addr}
	}

	// Drive every endpoint below MinThreshold using major errors, which do not trip the
	// critical-strike cooldown — this isolates the threshold path from the cooldown path.
	keyBuilder := repSvc.KeyBuilderForService(serviceID)
	for _, addr := range addrs {
		key := keyBuilder.BuildKey(serviceID, addr, rpcType)
		for range 8 {
			_ = repSvc.RecordSignal(ctx, key, reputation.Signal{
				Type:   reputation.SignalTypeMajorError,
				Reason: "health_check_critical_error",
			})
		}
		score, err := repSvc.GetScore(ctx, key)
		require.NoError(t, err)
		require.Less(t, score.Value, repConfig.MinThreshold,
			"endpoint %s must be below MinThreshold for this test to mean anything (got %.1f)", addr, score.Value)
	}

	result := p.filterToHighestTier(ctx, serviceID, endpoints, rpcType, logger, "")
	require.Equal(t, endpoints, result, "equally degraded candidates must survive the last tier filter")

	// A requested endpoint in the retained least-bad group also remains usable.
	result = p.filterToHighestTier(ctx, serviceID, endpoints, rpcType, logger, addrs[0])
	require.Equal(t, endpoints, result)
}

// TestShouldApplyTieredSelection covers the gate that decides whether the behavior pinned above
// runs at all. The Target-Suppliers case is the fix: a supplier pin must bypass tiered selection
// for the same reason it already bypasses the threshold/cooldown filter.
func TestShouldApplyTieredSelection(t *testing.T) {
	logger := polyzero.NewLogger()
	enabledSelector := reputation.NewTieredSelectorWithLogger(
		logger,
		reputation.TieredSelectionConfig{Enabled: true, Tier1Threshold: 80, Tier2Threshold: 50},
		20,
	)
	disabledSelector := reputation.NewTieredSelectorWithLogger(
		logger,
		reputation.TieredSelectionConfig{Enabled: false},
		20,
	)

	tests := []struct {
		name               string
		selector           *reputation.TieredSelector
		filterByReputation bool
		allowedSuppliers   []string
		rpcType            sharedtypes.RPCType
		want               bool
	}{
		{
			name:               "applies for a normal JSON-RPC request",
			selector:           enabledSelector,
			filterByReputation: true,
			rpcType:            sharedtypes.RPCType_JSON_RPC,
			want:               true,
		},
		{
			name:               "skipped when a Target-Suppliers pin is active",
			selector:           enabledSelector,
			filterByReputation: true,
			allowedSuppliers:   []string{"pokt1abc"},
			rpcType:            sharedtypes.RPCType_JSON_RPC,
			want:               false,
		},
		{
			name:               "skipped for health checks / leaderboard gathering",
			selector:           enabledSelector,
			filterByReputation: false,
			rpcType:            sharedtypes.RPCType_JSON_RPC,
			want:               false,
		},
		{
			name:               "skipped for WebSocket (S1)",
			selector:           enabledSelector,
			filterByReputation: true,
			rpcType:            sharedtypes.RPCType_WEBSOCKET,
			want:               false,
		},
		{
			name:               "skipped when tiered selection is disabled by config",
			selector:           disabledSelector,
			filterByReputation: true,
			rpcType:            sharedtypes.RPCType_JSON_RPC,
			want:               false,
		},
		{
			name:               "skipped when no selector is configured",
			selector:           nil,
			filterByReputation: true,
			rpcType:            sharedtypes.RPCType_JSON_RPC,
			want:               false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Protocol{tieredSelector: tt.selector}
			got := p.shouldApplyTieredSelection(tt.filterByReputation, tt.allowedSuppliers, tt.rpcType)
			require.Equal(t, tt.want, got)
		})
	}
}
