package controllers

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const testNodeName = "i-0123456789abcdef0"

type fakeMaxPodsSettings struct {
	current int32
	sets    []int32
}

func (f *fakeMaxPodsSettings) GetMaxPods(context.Context) (int32, error) { return f.current, nil }

func (f *fakeMaxPodsSettings) SetMaxPods(_ context.Context, maxPods int32) error {
	f.sets = append(f.sets, maxPods)
	f.current = maxPods
	return nil
}

type maxPodsHarness struct {
	t          *testing.T
	controller *maxPodsController
	settings   *fakeMaxPodsSettings
	now        time.Time
}

// newMaxPodsHarness creates this node's CNINode with status.maxPods set to maxPods (any
// JSON value, nil for unset) and kubelet configured with current max-pods.
func newMaxPodsHarness(t *testing.T, current int32, maxPods any) *maxPodsHarness {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(CNINodeGVK, &unstructured.Unstructured{})
	cniNode := NewCNINode()
	cniNode.SetName(testNodeName)
	if maxPods != nil {
		require.NoError(t, unstructured.SetNestedField(cniNode.Object, maxPods, "status", "maxPods"))
	}

	h := &maxPodsHarness{
		t:        t,
		settings: &fakeMaxPodsSettings{current: current},
		now:      time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
	}
	h.controller = NewMaxPodsController(
		fake.NewClientBuilder().WithScheme(scheme).WithObjects(cniNode).Build(),
		testNodeName, h.settings, filepath.Join(t.TempDir(), "max-pods.json"), 2*time.Minute)
	h.controller.now = func() time.Time { return h.now }
	return h
}

func (h *maxPodsHarness) reconcile() reconcile.Result {
	result, err := h.controller.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: testNodeName}})
	require.NoError(h.t, err)
	return result
}

// setMaxPods sets status.maxPods as the IPAM controller would; nil unsets it.
func (h *maxPodsHarness) setMaxPods(maxPods any) {
	cniNode := NewCNINode()
	require.NoError(h.t, h.controller.kubeClient.Get(context.Background(), types.NamespacedName{Name: testNodeName}, cniNode))
	if maxPods == nil {
		unstructured.RemoveNestedField(cniNode.Object, "status", "maxPods")
	} else {
		require.NoError(h.t, unstructured.SetNestedField(cniNode.Object, maxPods, "status", "maxPods"))
	}
	require.NoError(h.t, h.controller.kubeClient.Update(context.Background(), cniNode))
}

func TestDecideMaxPods(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	longAgo := now.Add(-time.Hour)
	recently := now.Add(-30 * time.Second)
	for name, tc := range map[string]struct {
		state    maxPodsState
		current  int32
		target   *int32
		expected maxPodsDecision
	}{
		"raises to the target":                     {maxPodsState{Baseline: 58, AppliedAt: longAgo}, 58, ptr(int32(74)), maxPodsDecision{desired: 74, apply: true}},
		"clamps to the ceiling":                    {maxPodsState{Baseline: 58}, 58, ptr(int32(500)), maxPodsDecision{desired: 110, apply: true}},
		"never goes below the baseline":            {maxPodsState{Baseline: 58}, 74, ptr(int32(20)), maxPodsDecision{desired: 58, apply: true}},
		"keeps a baseline above the ceiling":       {maxPodsState{Baseline: 234}, 234, ptr(int32(300)), maxPodsDecision{desired: 234}},
		"restores the baseline without a target":   {maxPodsState{Baseline: 58}, 74, nil, maxPodsDecision{desired: 58, apply: true}},
		"is a no-op when already applied":          {maxPodsState{Baseline: 58}, 74, ptr(int32(74)), maxPodsDecision{desired: 74}},
		"rate limits a raise":                      {maxPodsState{Baseline: 58, AppliedAt: recently}, 66, ptr(int32(74)), maxPodsDecision{desired: 74, requeueAfter: 90 * time.Second}},
		"never rate limits a lower":                {maxPodsState{Baseline: 58, AppliedAt: recently}, 82, ptr(int32(66)), maxPodsDecision{desired: 66, apply: true}},
		"never rate limits restoring the baseline": {maxPodsState{Baseline: 58, AppliedAt: recently}, 82, nil, maxPodsDecision{desired: 58, apply: true}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, decideMaxPods(tc.state, tc.current, tc.target, now, 2*time.Minute))
		})
	}
}

func TestParseApiclientMaxPods(t *testing.T) {
	value, err := parseApiclientMaxPods([]byte(`{"settings":{"kubernetes":{"max-pods":58}}}`))
	require.NoError(t, err)
	assert.Equal(t, int32(58), value)

	_, err = parseApiclientMaxPods([]byte(`{"settings":{"kubernetes":{}}}`))
	assert.Error(t, err)
}

func TestMaxPodsRaises(t *testing.T) {
	h := newMaxPodsHarness(t, 58, int64(74))
	h.reconcile()
	assert.Equal(t, []int32{74}, h.settings.sets)

	state, err := h.controller.loadState()
	require.NoError(t, err)
	assert.Equal(t, int32(58), state.Baseline, "baseline is the boot value, recorded before the first change")
}

func TestMaxPodsRateLimitsRaisesButNotLowers(t *testing.T) {
	h := newMaxPodsHarness(t, 58, int64(66))
	h.reconcile()

	h.now = h.now.Add(30 * time.Second)
	h.setMaxPods(int64(82))
	assert.Equal(t, 90*time.Second, h.reconcile().RequeueAfter)
	assert.Equal(t, []int32{66}, h.settings.sets)

	h.setMaxPods(int64(58))
	h.reconcile()
	assert.Equal(t, []int32{66, 58}, h.settings.sets)
}

func TestMaxPodsRestoresBaselineWhenMaxPodsUnset(t *testing.T) {
	h := newMaxPodsHarness(t, 58, int64(74))
	h.reconcile()

	h.setMaxPods(nil)
	h.reconcile()
	assert.Equal(t, []int32{74, 58}, h.settings.sets)
}

func TestMaxPodsDoesNotReapplyAfterRestart(t *testing.T) {
	// the apiclient commit restarted the agent after the setting was already applied.
	h := newMaxPodsHarness(t, 74, int64(74))
	require.NoError(t, h.controller.saveState(maxPodsState{Baseline: 58, Intent: 74, AppliedAt: h.now}))
	h.reconcile()
	assert.Empty(t, h.settings.sets)
}

func TestMaxPodsIgnoresMalformedMaxPods(t *testing.T) {
	for name, maxPods := range map[string]any{
		"out of int32 range": int64(2500000000),
		"zero":               int64(0),
		"not a number":       "lots",
	} {
		t.Run(name, func(t *testing.T) {
			h := newMaxPodsHarness(t, 58, maxPods)
			h.reconcile()
			assert.Empty(t, h.settings.sets)
		})
	}
}

func TestMaxPodsChangedPredicate(t *testing.T) {
	withMaxPods := func(maxPods any) *unstructured.Unstructured {
		obj := NewCNINode()
		if maxPods != nil {
			require.NoError(t, unstructured.SetNestedField(obj.Object, maxPods, "status", "maxPods"))
		}
		require.NoError(t, unstructured.SetNestedField(obj.Object, "eni-1", "status", "networkInterfaces"))
		return obj
	}
	update := func(oldObj, newObj *unstructured.Unstructured) event.TypedUpdateEvent[client.Object] {
		return event.TypedUpdateEvent[client.Object]{ObjectOld: oldObj, ObjectNew: newObj}
	}
	assert.True(t, maxPodsChanged(update(withMaxPods(int64(56)), withMaxPods(int64(64)))))
	assert.True(t, maxPodsChanged(update(withMaxPods(int64(64)), withMaxPods(nil))))
	newObj := withMaxPods(int64(64))
	require.NoError(t, unstructured.SetNestedField(newObj.Object, "eni-2", "status", "networkInterfaces"))
	assert.False(t, maxPodsChanged(update(withMaxPods(int64(64)), newObj)), "other status changes are ignored")
}

func TestMaxPodsLeavesUntouchedNodesAlone(t *testing.T) {
	h := newMaxPodsHarness(t, 58, nil)
	h.reconcile()
	assert.Empty(t, h.settings.sets)
	assert.NoFileExists(t, h.controller.statePath)
}
