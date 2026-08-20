package shannon

import (
	"testing"

	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"
)

// prefer_probed is the request-path counterpart of max_probe_endpoints. If we
// probe only the top N, the other 48 stay at initial_score 80 and LOOK healthy;
// without this filter they win user traffic. Tests go through Survivors() — the
// same filterByReputation hedge, retry and HTTP/WS selection all call.

func TestSelection_PreferProbed_HighScoreUnprobedLosesToProbedHealthy(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	// Unprobed sits well above the probed-healthy endpoint — the exact failure
	// mode of "probe 2 of 50, leave the rest at initial_score".
	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_JSON_RPC, 15) // 80+15 = 95
	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)                // 80+5  = 85, probed

	s.AssertServes(sharedtypes.RPCType_JSON_RPC, opGammaA)
	s.AssertExcluded(sharedtypes.RPCType_JSON_RPC, opBetaA)
}

func TestSelection_PreferProbed_ZeroProbedAllowsUnprobed(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	// Startup / just after a roll that introduced new keys: nothing has been
	// probed yet, so the pool must not go empty. The next HC cycle fills it.
	s.AssertServes(sharedtypes.RPCType_JSON_RPC, opBetaA, opGammaA)
}

func TestSelection_PreferProbedOff_UnprobedStaysSelectable(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	// preferProbed stays false — today's behavior, and the merge-safe default.

	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_JSON_RPC, 15)
	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)

	s.AssertSelectable(sharedtypes.RPCType_JSON_RPC, opBetaA, opGammaA)
}

func TestSelection_PreferProbed_JSONRPCIgnoresWebsocketProbe(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_JSON_RPC, 15)
	s.Probe(opGammaA, sharedtypes.RPCType_WEBSOCKET)

	// A websocket probe must not mark the json_rpc key. json_rpc traffic still
	// sees zero probed endpoints and keeps the unprobed set.
	s.AssertServes(sharedtypes.RPCType_JSON_RPC, opBetaA, opGammaA)
}

func TestSelection_PreferProbed_JSONRPCProbeDoesNotAffectWebsocket(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_WEBSOCKET, 15)
	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)

	s.AssertServes(sharedtypes.RPCType_WEBSOCKET, opBetaA, opGammaA)
}

func TestSelection_PreferProbed_ProbedInCooldownAllowsUnprobed(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)
	s.DriveIntoCooldown(opGammaA, sharedtypes.RPCType_JSON_RPC)

	// The only probed endpoint is not selectable (cooldown). Unprobed must
	// remain reachable — otherwise prefer_probed would trap traffic on a
	// cooled node while healthy unprobed ones sit unused.
	s.AssertExcluded(sharedtypes.RPCType_JSON_RPC, opGammaA)
	s.AssertSelectable(sharedtypes.RPCType_JSON_RPC, opBetaA)
}

func TestSelection_PreferProbed_SurvivesSupplierRotation(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_JSON_RPC, 15)
	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)
	s.AssertExcluded(sharedtypes.RPCType_JSON_RPC, opBetaA)

	// Default key granularity is per-url, so a session rollover that changes
	// the supplier address keeps the same probed key. Unprobed still loses.
	s.RotateSuppliers(1)
	s.AssertServes(sharedtypes.RPCType_JSON_RPC, opGammaA)
	s.AssertExcluded(sharedtypes.RPCType_JSON_RPC, opBetaA)
}

func TestSelection_PreferProbed_PreferredEndpointIsNotExempt(t *testing.T) {
	s := newSelectionScenario(t, "eth", opBetaA, opGammaA)
	s.p.preferProbed = true

	s.RecordUserSuccess(opBetaA, sharedtypes.RPCType_JSON_RPC, 15)
	s.Probe(opGammaA, sharedtypes.RPCType_JSON_RPC)

	// Hedge/retry/WS rebind pass the currently bound endpoint as preferred.
	// An unprobed preferred endpoint must still lose — the same rule that
	// defeated drains when they exempted requestedEndpointAddr.
	got := s.SurvivorsPreferring(sharedtypes.RPCType_JSON_RPC, opBetaA)
	require.Equal(t, []string{opGammaA}, got)
}
