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

func newMaxPodsHarness(t *testing.T, current int32, annotations map[string]string) *maxPodsHarness {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(CNINodeGVK, &unstructured.Unstructured{})
	cniNode := &unstructured.Unstructured{}
	cniNode.SetGroupVersionKind(CNINodeGVK)
	cniNode.SetName(testNodeName)
	cniNode.SetAnnotations(annotations)

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

func (h *maxPodsHarness) annotations() map[string]string {
	cniNode := NewCNINodeMetadata()
	require.NoError(h.t, h.controller.kubeClient.Get(context.Background(), types.NamespacedName{Name: testNodeName}, cniNode))
	return cniNode.GetAnnotations()
}

func (h *maxPodsHarness) setTarget(value string) {
	cniNode := NewCNINodeMetadata()
	require.NoError(h.t, h.controller.kubeClient.Get(context.Background(), types.NamespacedName{Name: testNodeName}, cniNode))
	patch := client.MergeFrom(cniNode.DeepCopy())
	annotations := cniNode.GetAnnotations()
	if value == "" {
		delete(annotations, TargetMaxPodsAnnotation)
	} else {
		annotations = mergeMaps(annotations, map[string]string{TargetMaxPodsAnnotation: value})
	}
	cniNode.SetAnnotations(annotations)
	require.NoError(h.t, h.controller.kubeClient.Patch(context.Background(), cniNode, patch))
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

func TestMaxPodsRaisesAndAcknowledges(t *testing.T) {
	h := newMaxPodsHarness(t, 58, map[string]string{TargetMaxPodsAnnotation: "74"})
	h.reconcile()
	assert.Equal(t, []int32{74}, h.settings.sets)
	assert.Equal(t, "74", h.annotations()[AppliedMaxPodsAnnotation])

	state, err := h.controller.loadState()
	require.NoError(t, err)
	assert.Equal(t, int32(58), state.Baseline, "baseline is the boot value, recorded before the first change")
}

func TestMaxPodsRateLimitsRaisesButNotLowers(t *testing.T) {
	h := newMaxPodsHarness(t, 58, map[string]string{TargetMaxPodsAnnotation: "66"})
	h.reconcile()

	h.now = h.now.Add(30 * time.Second)
	h.setTarget("82")
	assert.Equal(t, 90*time.Second, h.reconcile().RequeueAfter)
	assert.Equal(t, []int32{66}, h.settings.sets)

	h.setTarget("58")
	h.reconcile()
	assert.Equal(t, []int32{66, 58}, h.settings.sets)
	assert.Equal(t, "58", h.annotations()[AppliedMaxPodsAnnotation])
}

func TestMaxPodsRestoresBaselineWhenTargetRemoved(t *testing.T) {
	h := newMaxPodsHarness(t, 58, map[string]string{TargetMaxPodsAnnotation: "74"})
	h.reconcile()

	h.setTarget("")
	h.reconcile()
	assert.Equal(t, []int32{74, 58}, h.settings.sets)
	assert.Equal(t, "58", h.annotations()[AppliedMaxPodsAnnotation])
}

func TestMaxPodsAcknowledgesAfterRestart(t *testing.T) {
	// the apiclient commit restarted the agent before it could acknowledge.
	h := newMaxPodsHarness(t, 74, map[string]string{TargetMaxPodsAnnotation: "74"})
	require.NoError(t, h.controller.saveState(maxPodsState{Baseline: 58, Intent: 74, AppliedAt: h.now}))
	h.reconcile()
	assert.Empty(t, h.settings.sets)
	assert.Equal(t, "74", h.annotations()[AppliedMaxPodsAnnotation])
}

func TestMaxPodsIgnoresMalformedTarget(t *testing.T) {
	h := newMaxPodsHarness(t, 58, map[string]string{TargetMaxPodsAnnotation: "2500000000"})
	h.reconcile()
	assert.Empty(t, h.settings.sets)
	assert.NotContains(t, h.annotations(), AppliedMaxPodsAnnotation)
}

func TestMaxPodsLeavesUntouchedNodesAlone(t *testing.T) {
	h := newMaxPodsHarness(t, 58, nil)
	h.reconcile()
	assert.Empty(t, h.settings.sets)
	assert.NoFileExists(t, h.controller.statePath)
}
