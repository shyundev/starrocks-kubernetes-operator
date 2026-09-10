/*
Copyright 2021-present, StarRocks Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fe

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	srapi "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/load"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/templates/pod"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/templates/statefulset"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/subcontrollers"
)

const (
	// LeaderAwareRollingUpdateEventReason is the reason of every event the leader aware rolling update records.
	LeaderAwareRollingUpdateEventReason = "LeaderAwareRollingUpdate"

	// leaderAwareRollingUpdatePollInterval bounds how long a replaced pod's start, or its first heartbeat to
	// the leader, can go unnoticed. Pod and statefulset events wake the reconciler earlier; this is the fallback.
	leaderAwareRollingUpdatePollInterval = 10 * time.Second
	// leaderAwareRollingUpdateRetryInterval is used when SHOW FRONTENDS fails. That usually needs a person to
	// fix the root password, so polling faster gains nothing.
	leaderAwareRollingUpdateRetryInterval = 30 * time.Second
)

// frontendPod pairs an FE pod with what SHOW FRONTENDS reports about it.
type frontendPod struct {
	pod     corev1.Pod
	ordinal int
	// stale is true when the pod does not run the update revision of the statefulset.
	stale bool
	// frontend is nil while FE does not list the pod.
	frontend *Frontend
}

// FrontendClientFactory builds the FE client used by the leader aware rolling update.
type FrontendClientFactory func(ctx context.Context, k8sClient client.Client, src *srapi.StarRocksCluster,
	feConfig map[string]interface{}) (FrontendClient, error)

func defaultFrontendClientFactory(ctx context.Context, k8sClient client.Client, src *srapi.StarRocksCluster,
	feConfig map[string]interface{}) (FrontendClient, error) {
	frontendClient, err := NewSQLFrontendClient(ctx, k8sClient, src, feConfig)
	if err != nil {
		return nil, err
	}
	return frontendClient, nil
}

// reconcileLeaderAwareRollingUpdate replaces the FE pods that do not run the update revision of the FE statefulset,
// one at a time: followers and observers first, the leader last. The statefulset uses the OnDelete strategy, so a
// pod only restarts with the new revision when the operator deletes it. The function returns a RequeueError while
// the update is in progress and nil once every pod runs the update revision.
func (fc *FeController) reconcileLeaderAwareRollingUpdate(ctx context.Context, src *srapi.StarRocksCluster,
	feConfig map[string]interface{}) error {
	logger := logr.FromContextOrDiscard(ctx)
	feSpec := src.Spec.StarRocksFeSpec

	var sts appsv1.StatefulSet
	if err := fc.Client.Get(ctx,
		types.NamespacedName{Namespace: src.Namespace, Name: load.Name(src.Name, feSpec)}, &sts); err != nil {
		return err
	}
	if sts.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType ||
		sts.Status.ObservedGeneration < sts.Generation || sts.Status.UpdateRevision == "" {
		return requeueRollingUpdate(leaderAwareRollingUpdatePollInterval,
			"waiting for the statefulset controller to observe the FE statefulset")
	}

	var podList corev1.PodList
	if err := fc.Client.List(ctx, &podList, client.InNamespace(src.Namespace),
		client.MatchingLabels(pod.Labels(src.Name, feSpec))); err != nil {
		return err
	}
	pods, err := frontendPods(podList.Items, sts.Status.UpdateRevision)
	if err != nil {
		return err
	}
	if !anyStale(pods) {
		return nil
	}
	if reason := waitForPods(pods, int(statefulset.DesiredReplicas(&sts))); reason != "" {
		return requeueRollingUpdate(leaderAwareRollingUpdatePollInterval, reason)
	}

	frontendClient, err := fc.frontendClient(ctx, src, feConfig)
	if err != nil {
		return err
	}
	frontends, err := frontendClient.ShowFrontends(ctx)
	if err != nil {
		msg := fmt.Sprintf("%s failed, the FE rolling update is paused until it succeeds "+
			"(when root has a password, set MYSQL_PWD in feEnvVars): %v", ShowFrontendsStatement, err)
		fc.Recorder.Event(src, corev1.EventTypeWarning, LeaderAwareRollingUpdateEventReason, msg)
		return requeueRollingUpdate(leaderAwareRollingUpdateRetryInterval, msg)
	}
	attachFrontends(pods, frontends)
	if reason := waitForFrontends(pods); reason != "" {
		return requeueRollingUpdate(leaderAwareRollingUpdatePollInterval, reason)
	}

	victim := nextPodToReplace(pods)
	if victim.frontend.Role == FrontendRoleLeader {
		fc.transferLeaderBeforeReplacing(ctx, src, frontendClient, pods, victim)
	}
	if err = fc.Client.Delete(ctx, &victim.pod, client.Preconditions{UID: &victim.pod.UID}); err != nil &&
		!apierrors.IsNotFound(err) {
		return err
	}
	msg := fmt.Sprintf("deleted pod %s (%s) so that it restarts at revision %s",
		victim.pod.Name, victim.frontend.Role, sts.Status.UpdateRevision)
	logger.Info(msg)
	fc.Recorder.Event(src, corev1.EventTypeNormal, LeaderAwareRollingUpdateEventReason, msg)
	return requeueRollingUpdate(leaderAwareRollingUpdatePollInterval,
		"waiting for pod "+victim.pod.Name+" to be replaced")
}

func (fc *FeController) frontendClient(ctx context.Context, src *srapi.StarRocksCluster,
	feConfig map[string]interface{}) (FrontendClient, error) {
	factory := fc.frontendClientFactory
	if factory == nil {
		factory = defaultFrontendClientFactory
	}
	return factory(ctx, fc.Client, src, feConfig)
}

// transferLeaderBeforeReplacing hands leadership to an already updated follower so that replacing the leader pod
// does not cost an election. It is best effort: FE versions without ALTER SYSTEM TRANSFER LEADER, shared-data
// clusters and a missing candidate all fall back to restarting the leader, after which FE elects a new one.
func (fc *FeController) transferLeaderBeforeReplacing(ctx context.Context, src *srapi.StarRocksCluster,
	frontendClient FrontendClient, pods []*frontendPod, leader *frontendPod) {
	logger := logr.FromContextOrDiscard(ctx)
	target := leaderTransferTarget(pods)
	if target == nil {
		msg := fmt.Sprintf("no updated follower FE to transfer the leader role to before replacing pod %s, "+
			"a leader election will follow", leader.pod.Name)
		logger.Info(msg)
		fc.Recorder.Event(src, corev1.EventTypeNormal, LeaderAwareRollingUpdateEventReason, msg)
		return
	}
	if err := frontendClient.TransferLeader(ctx, *target.frontend); err != nil {
		msg := fmt.Sprintf("transferring the leader role to pod %s before replacing pod %s failed, "+
			"a leader election will follow: %v", target.pod.Name, leader.pod.Name, err)
		logger.Info(msg)
		fc.Recorder.Event(src, corev1.EventTypeNormal, LeaderAwareRollingUpdateEventReason, msg)
		return
	}
	msg := fmt.Sprintf("transferred the leader role from pod %s to pod %s", leader.pod.Name, target.pod.Name)
	logger.Info(msg)
	fc.Recorder.Event(src, corev1.EventTypeNormal, LeaderAwareRollingUpdateEventReason, msg)
}

func requeueRollingUpdate(after time.Duration, reason string) error {
	return &subcontrollers.RequeueError{After: after, Reason: reason}
}

// frontendPods sorts the pods by ordinal, highest first, which is the order the statefulset controller itself
// replaces pods in, and marks the ones that do not run the update revision.
func frontendPods(items []corev1.Pod, updateRevision string) ([]*frontendPod, error) {
	pods := make([]*frontendPod, 0, len(items))
	for i := range items {
		ordinal, err := podOrdinal(items[i].Name)
		if err != nil {
			return nil, err
		}
		pods = append(pods, &frontendPod{
			pod:     items[i],
			ordinal: ordinal,
			stale:   items[i].Labels[appsv1.ControllerRevisionHashLabelKey] != updateRevision,
		})
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].ordinal > pods[j].ordinal })
	return pods, nil
}

func podOrdinal(name string) (int, error) {
	i := strings.LastIndex(name, "-")
	if i < 0 {
		return 0, fmt.Errorf("pod %s is not named like a statefulset pod", name)
	}
	ordinal, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return 0, fmt.Errorf("pod %s is not named like a statefulset pod: %w", name, err)
	}
	return ordinal, nil
}

func anyStale(pods []*frontendPod) bool {
	for _, p := range pods {
		if p.stale {
			return true
		}
	}
	return false
}

// waitForPods returns why the update cannot continue right now: a pod is missing (it is being recreated or the
// statefulset is scaling), terminating or not Ready. Only one pod is ever replaced at a time, so this is also what
// serializes the update.
func waitForPods(pods []*frontendPod, desired int) string {
	if len(pods) != desired {
		return fmt.Sprintf("waiting for %d FE pods, found %d", desired, len(pods))
	}
	for _, p := range pods {
		if p.pod.DeletionTimestamp != nil {
			return "waiting for pod " + p.pod.Name + " to terminate"
		}
		if !k8sutils.PodIsReady(&p.pod.Status) {
			return "waiting for pod " + p.pod.Name + " to be ready"
		}
	}
	return ""
}

// attachFrontends matches the SHOW FRONTENDS rows to the pods. FE reports the host it was started with: the pod
// FQDN, <pod>.<search service>.<namespace>.svc.<domain>, with the default HOST_TYPE=FQDN, the pod IP otherwise.
func attachFrontends(pods []*frontendPod, frontends []Frontend) {
	for i := range frontends {
		for _, p := range pods {
			if frontends[i].Host == p.pod.Status.PodIP || strings.HasPrefix(frontends[i].Host, p.pod.Name+".") {
				p.frontend = &frontends[i]
			}
		}
	}
}

// waitForFrontends returns why FE is not ready for the next replacement: FE does not list a pod or does not see it
// alive yet (a replaced FE is Ready before the leader's next heartbeat reaches it), or there is no leader because
// an election is running.
func waitForFrontends(pods []*frontendPod) string {
	hasLeader := false
	for _, p := range pods {
		if p.frontend == nil {
			return "waiting for FE to list pod " + p.pod.Name
		}
		if !p.frontend.Alive {
			return "waiting for the FE of pod " + p.pod.Name + " to be alive"
		}
		if p.frontend.Role == FrontendRoleLeader {
			hasLeader = true
		}
	}
	if !hasLeader {
		return "waiting for a leader FE to be elected"
	}
	return ""
}

// nextPodToReplace picks the stale pod to delete: followers and observers first, highest ordinal first, and the
// leader only when nothing else is left. Every pod has a frontend here, waitForFrontends guarantees it.
func nextPodToReplace(pods []*frontendPod) *frontendPod {
	var leader *frontendPod
	for _, p := range pods {
		if !p.stale {
			continue
		}
		if p.frontend.Role == FrontendRoleLeader {
			leader = p
			continue
		}
		return p
	}
	return leader
}

// leaderTransferTarget is the updated, alive follower with the highest ordinal.
func leaderTransferTarget(pods []*frontendPod) *frontendPod {
	for _, p := range pods {
		if !p.stale && p.frontend != nil && p.frontend.Role == FrontendRoleFollower && p.frontend.Alive {
			return p
		}
	}
	return nil
}
