package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// MaxPodsCeiling bounds what is applied regardless of status.maxPods; an unbounded value breaks kubelet.
	MaxPodsCeiling = int32(110)

	DefaultMaxPodsStatePath   = "/var/lib/eks-node-monitoring-agent/max-pods.json"
	DefaultMaxPodsMinInterval = 2 * time.Minute
)

// CNINodeGVK identifies the EKS Auto CNINode, read as unstructured to avoid depending on its Go types.
var CNINodeGVK = schema.GroupVersionKind{Group: "eks.amazonaws.com", Version: "v1alpha1", Kind: "CNINode"}

// NewCNINode returns an empty unstructured CNINode, usable for watches and cache options.
func NewCNINode() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(CNINodeGVK)
	return obj
}

// cniNodeMaxPods reads status.maxPods. ok is false when it is unset; err is set when the
// value is not a usable max-pods.
func cniNodeMaxPods(obj *unstructured.Unstructured) (value int32, ok bool, err error) {
	raw, found, err := unstructured.NestedInt64(obj.Object, "status", "maxPods")
	if err != nil || !found {
		return 0, false, err
	}
	if raw <= 0 || raw > math.MaxInt32 {
		return 0, false, fmt.Errorf("status.maxPods %d is out of range", raw)
	}
	return int32(raw), true, nil
}

// maxPodsChanged passes CNINode updates only when status.maxPods changes; IPAM rewrites the
// rest of the status on every IP allocation.
func maxPodsChanged(e event.TypedUpdateEvent[client.Object]) bool {
	oldObj, oldOK := e.ObjectOld.(*unstructured.Unstructured)
	newObj, newOK := e.ObjectNew.(*unstructured.Unstructured)
	if !oldOK || !newOK {
		return true
	}
	oldValue, _, _ := unstructured.NestedFieldNoCopy(oldObj.Object, "status", "maxPods")
	newValue, _, _ := unstructured.NestedFieldNoCopy(newObj.Object, "status", "maxPods")
	return !reflect.DeepEqual(oldValue, newValue)
}

// MaxPodsSettings reads and writes the kubelet max-pods setting on the host.
type MaxPodsSettings interface {
	GetMaxPods(ctx context.Context) (int32, error)
	// SetMaxPods commits the setting. The commit can restart this agent, so persist intent first.
	SetMaxPods(ctx context.Context, maxPods int32) error
}

// maxPodsState survives agent restarts, including the one triggered by SetMaxPods.
type maxPodsState struct {
	// Baseline is the boot max-pods, recorded before the first change: the floor, and the value
	// restored when status.maxPods is unset.
	Baseline int32 `json:"baseline"`
	Intent   int32 `json:"intent,omitempty"`
	// AppliedAt rate limits raises.
	AppliedAt time.Time `json:"appliedAt,omitempty"`
}

type maxPodsDecision struct {
	desired      int32
	apply        bool
	requeueAfter time.Duration
}

// decideMaxPods clamps the target to [baseline, ceiling], restores the baseline when there is
// no target, and rate limits raises (each change restarts kubelet); lowers apply right away.
func decideMaxPods(state maxPodsState, current int32, target *int32, now time.Time, minInterval time.Duration) maxPodsDecision {
	desired := state.Baseline
	if target != nil {
		desired = min(max(*target, state.Baseline), max(state.Baseline, MaxPodsCeiling))
	}
	if desired == current {
		return maxPodsDecision{desired: desired}
	}
	if desired > current {
		if wait := minInterval - now.Sub(state.AppliedAt); wait > 0 {
			return maxPodsDecision{desired: desired, requeueAfter: wait}
		}
	}
	return maxPodsDecision{desired: desired, apply: true}
}

type maxPodsController struct {
	kubeClient  client.Client
	nodeName    string
	settings    MaxPodsSettings
	statePath   string
	minInterval time.Duration
	now         func() time.Time
}

// NewMaxPodsController applies the max-pods the IPAM controller publishes in this node's
// CNINode status.maxPods to kubelet.
func NewMaxPodsController(kubeClient client.Client, nodeName string, settings MaxPodsSettings, statePath string, minInterval time.Duration) *maxPodsController {
	return &maxPodsController{
		kubeClient:  kubeClient,
		nodeName:    nodeName,
		settings:    settings,
		statePath:   statePath,
		minInterval: minInterval,
		now:         time.Now,
	}
}

func (c *maxPodsController) Register(ctx context.Context, m controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("max-pods").
		For(NewCNINode()).
		WithEventFilter(predicate.And(
			// only action the CNINode corresponding to the controller node.
			predicate.NewPredicateFuncs(func(object client.Object) bool { return object.GetName() == c.nodeName }),
			predicate.Funcs{
				UpdateFunc: maxPodsChanged,
				DeleteFunc: func(event.TypedDeleteEvent[client.Object]) bool { return false },
			},
		)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(c)
}

func (c *maxPodsController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithValues("cninode", req.Name)

	cniNode := NewCNINode()
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: req.Name}, cniNode); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	var target *int32
	value, ok, err := cniNodeMaxPods(cniNode)
	if err != nil {
		logger.Error(err, "ignoring malformed CNINode status.maxPods")
		return reconcile.Result{}, nil
	}
	if ok {
		target = ptr(value)
	}

	current, err := c.settings.GetMaxPods(ctx)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("reading max pods setting, %w", err)
	}

	state, err := c.loadState()
	if err != nil {
		return reconcile.Result{}, err
	}
	if state.Baseline == 0 {
		if target == nil {
			return reconcile.Result{}, nil
		}
		state.Baseline = current
		if err := c.saveState(state); err != nil {
			return reconcile.Result{}, err
		}
	}

	decision := decideMaxPods(state, current, target, c.now(), c.minInterval)
	if decision.requeueAfter > 0 {
		logger.V(2).Info("rate limiting max pods raise", "current", current, "desired", decision.desired, "after", decision.requeueAfter)
		return reconcile.Result{RequeueAfter: decision.requeueAfter}, nil
	}
	if !decision.apply {
		return reconcile.Result{}, nil
	}

	state.Intent, state.AppliedAt = decision.desired, c.now()
	// persist first: the commit restarts this agent, possibly before SetMaxPods returns.
	if err := c.saveState(state); err != nil {
		return reconcile.Result{}, err
	}
	logger.Info("applying max pods", "current", current, "desired", decision.desired, "target", target, "baseline", state.Baseline)
	if err := c.settings.SetMaxPods(ctx, decision.desired); err != nil {
		return reconcile.Result{}, fmt.Errorf("setting max pods to %d, %w", decision.desired, err)
	}
	if current, err = c.settings.GetMaxPods(ctx); err != nil {
		return reconcile.Result{}, fmt.Errorf("verifying max pods setting, %w", err)
	}
	if current != decision.desired {
		return reconcile.Result{}, fmt.Errorf("max pods setting is %d after applying %d", current, decision.desired)
	}
	return reconcile.Result{}, nil
}

func (c *maxPodsController) loadState() (maxPodsState, error) {
	var state maxPodsState
	data, err := os.ReadFile(c.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	} else if err != nil {
		return state, fmt.Errorf("reading max pods state, %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decoding max pods state, %w", err)
	}
	return state, nil
}

// saveState writes atomically so a restart mid-write never leaves a truncated baseline.
func (c *maxPodsController) saveState(state maxPodsState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.statePath), 0o700); err != nil {
		return fmt.Errorf("creating max pods state dir, %w", err)
	}
	tmp := c.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("writing max pods state, %w", err)
	}
	return os.Rename(tmp, c.statePath)
}

// ApiclientMaxPodsSettings drives the Bottlerocket API through the apiclient binary.
type ApiclientMaxPodsSettings struct {
	Path string
}

func (a ApiclientMaxPodsSettings) GetMaxPods(ctx context.Context) (int32, error) {
	out, err := exec.CommandContext(ctx, a.Path, "get", "settings.kubernetes.max-pods").Output()
	if err != nil {
		return 0, fmt.Errorf("apiclient get, %w", err)
	}
	return parseApiclientMaxPods(out)
}

func (a ApiclientMaxPodsSettings) SetMaxPods(ctx context.Context, maxPods int32) error {
	if out, err := exec.CommandContext(ctx, a.Path, "set", fmt.Sprintf("settings.kubernetes.max-pods=%d", maxPods)).CombinedOutput(); err != nil {
		return fmt.Errorf("apiclient set, %w: %s", err, out)
	}
	return nil
}

// parseApiclientMaxPods reads `{"settings":{"kubernetes":{"max-pods":58}}}`.
func parseApiclientMaxPods(out []byte) (int32, error) {
	var resp struct {
		Settings struct {
			Kubernetes struct {
				MaxPods *int32 `json:"max-pods"`
			} `json:"kubernetes"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return 0, fmt.Errorf("decoding apiclient output, %w", err)
	}
	if resp.Settings.Kubernetes.MaxPods == nil {
		return 0, errors.New("settings.kubernetes.max-pods is not set")
	}
	return *resp.Settings.Kubernetes.MaxPods, nil
}

func ptr[T any](v T) *T { return &v }
