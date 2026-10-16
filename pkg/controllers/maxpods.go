package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// TargetMaxPodsAnnotation is published on the node's CNINode by the EKS Auto IPAM
	// controller with the kubelet max-pods its currently assigned IPs can back.
	TargetMaxPodsAnnotation = "eks.amazonaws.com/target-max-pods"

	// MaxPodsCeiling is the highest max-pods this controller will ever apply, regardless
	// of what is published. Setting an unbounded value through apiclient breaks kubelet.
	MaxPodsCeiling = int32(110)

	DefaultMaxPodsStatePath   = "/var/lib/eks-node-monitoring-agent/max-pods.json"
	DefaultMaxPodsMinInterval = 2 * time.Minute
)

// CNINodeGVK identifies the EKS Auto CNINode. The controller only reads metadata, so it
// does not need the CNINode Go types.
var CNINodeGVK = schema.GroupVersionKind{Group: "eks.amazonaws.com", Version: "v1alpha1", Kind: "CNINode"}

// NewCNINodeMetadata returns an empty metadata-only CNINode, usable for watches and cache options.
func NewCNINodeMetadata() *metav1.PartialObjectMetadata {
	obj := &metav1.PartialObjectMetadata{}
	obj.SetGroupVersionKind(CNINodeGVK)
	return obj
}

// MaxPodsSettings reads and writes the kubelet max-pods setting on the host.
type MaxPodsSettings interface {
	GetMaxPods(ctx context.Context) (int32, error)
	// SetMaxPods commits the setting. On Bottlerocket this restarts every kubernetes
	// service, including this agent, so callers must persist intent before calling it.
	SetMaxPods(ctx context.Context, maxPods int32) error
}

// maxPodsState survives agent restarts, including the one triggered by SetMaxPods.
type maxPodsState struct {
	// Baseline is the max-pods the node booted with (set by Karpenter user data), recorded
	// before this controller first changes it. It is both the floor and the value restored
	// when the target annotation is removed.
	Baseline int32 `json:"baseline"`
	// Intent is the last value this controller asked apiclient to apply.
	Intent int32 `json:"intent,omitempty"`
	// AppliedAt is when Intent was requested; it rate limits raises.
	AppliedAt time.Time `json:"appliedAt,omitempty"`
}

type maxPodsDecision struct {
	desired      int32
	apply        bool
	requeueAfter time.Duration
}

// decideMaxPods is the pure policy: clamp the published target to [baseline, ceiling],
// restore the baseline when no target is published, apply lowers right away, and rate
// limit raises so repeated kubelet restarts cannot pile up.
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

// NewMaxPodsController applies the IPAM controller's per-node max-pods target to kubelet.
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
		For(NewCNINodeMetadata()).
		WithEventFilter(predicate.And(
			// only action the CNINode corresponding to the controller node.
			predicate.NewPredicateFuncs(func(object client.Object) bool { return object.GetName() == c.nodeName }),
			// IPAMC writes status constantly; only the annotations carry the target.
			predicate.AnnotationChangedPredicate{},
			predicate.Funcs{DeleteFunc: func(event.TypedDeleteEvent[client.Object]) bool { return false }},
		)).
		WithOptions(controller.Options{
			// apply one value at a time.
			MaxConcurrentReconciles: 1,
		}).
		Complete(c)
}

func (c *maxPodsController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithValues("cninode", req.Name)

	cniNode := NewCNINodeMetadata()
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: req.Name}, cniNode); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	var target *int32
	if raw, ok := cniNode.GetAnnotations()[TargetMaxPodsAnnotation]; ok {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || value <= 0 {
			// never act on a value we cannot trust; leave kubelet where it is.
			logger.Error(err, "ignoring malformed target max pods", "value", raw)
			return reconcile.Result{}, nil
		}
		target = ptr(int32(value))
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
			// nothing published and nothing changed yet; no reason to touch disk.
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
	// kubelet reports the applied value as the Node's allocatable pods, so nothing is written back.
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
