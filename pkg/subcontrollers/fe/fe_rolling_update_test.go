package fe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	srapi "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	rutils "github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/resource_utils"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/fake"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/subcontrollers"
)

const (
	testClusterName = "kube-starrocks"
	testNamespace   = "default"
	oldRevision     = "kube-starrocks-fe-1111111111"
	newRevision     = "kube-starrocks-fe-2222222222"
)

// fakeFrontendClient scripts SHOW FRONTENDS and records leader transfers.
type fakeFrontendClient struct {
	frontends     []Frontend
	showErr       error
	transferErr   error
	transferredTo []Frontend
}

func (f *fakeFrontendClient) ShowFrontends(_ context.Context) ([]Frontend, error) {
	return f.frontends, f.showErr
}

func (f *fakeFrontendClient) TransferLeader(_ context.Context, target Frontend) error {
	f.transferredTo = append(f.transferredTo, target)
	return f.transferErr
}

func testCluster(replicas int32) *srapi.StarRocksCluster {
	return &srapi.StarRocksCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
		Spec: srapi.StarRocksClusterSpec{
			StarRocksFeSpec: &srapi.StarRocksFeSpec{
				StarRocksComponentSpec: srapi.StarRocksComponentSpec{
					StarRocksLoadSpec: srapi.StarRocksLoadSpec{
						Replicas: rutils.GetInt32Pointer(replicas),
						Image:    "starrocks/fe-ubuntu:3.5.4",
					},
				},
				LeaderAwareRollingUpdate: true,
			},
		},
	}
}

func testStatefulSet(replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{Kind: "StatefulSet", APIVersion: appsv1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-fe", Namespace: testNamespace, Generation: 2},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			CurrentRevision:    oldRevision,
			UpdateRevision:     newRevision,
		},
	}
}

func fePodName(ordinal int) string {
	return fmt.Sprintf("%s-fe-%d", testClusterName, ordinal)
}

func feHost(ordinal int) string {
	return fmt.Sprintf("%s.%s-fe-search.%s.svc.cluster.local", fePodName(ordinal), testClusterName, testNamespace)
}

func testPod(ordinal int, revision string, ready bool) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fePodName(ordinal),
			Namespace: testNamespace,
			UID:       types.UID(fmt.Sprintf("uid-%d-%s", ordinal, revision)),
			Labels: map[string]string{
				srapi.OwnerReference:                  testClusterName + "-fe",
				srapi.ComponentLabelKey:               srapi.DEFAULT_FE,
				appsv1.ControllerRevisionHashLabelKey: revision,
			},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			PodIP:             fmt.Sprintf("10.0.0.%d", ordinal+1),
			ContainerStatuses: []corev1.ContainerStatus{{Ready: ready}},
		},
	}
}

func frontend(ordinal int, role string) Frontend {
	return Frontend{Host: feHost(ordinal), EditLogPort: "9010", Role: role, Alive: true}
}

type rollingUpdateFixture struct {
	controller *FeController
	client     client.Client
	recorder   *record.FakeRecorder
	frontends  *fakeFrontendClient
	src        *srapi.StarRocksCluster
}

func newRollingUpdateFixture(t *testing.T, frontends *fakeFrontendClient, objects ...runtime.Object) *rollingUpdateFixture {
	t.Helper()
	srapi.Register()
	recorder := record.NewFakeRecorder(32)
	k8sClient := fake.NewFakeClient(srapi.Scheme, objects...)
	controller := New(k8sClient, fake.GetEventRecorderFor(recorder))
	controller.frontendClientFactory = func(_ context.Context, _ client.Client, _ *srapi.StarRocksCluster,
		_ map[string]interface{}) (FrontendClient, error) {
		return frontends, nil
	}
	return &rollingUpdateFixture{
		controller: controller,
		client:     k8sClient,
		recorder:   recorder,
		frontends:  frontends,
		src:        testCluster(3),
	}
}

func (f *rollingUpdateFixture) reconcile(t *testing.T) error {
	t.Helper()
	return f.controller.reconcileLeaderAwareRollingUpdate(context.Background(), f.src, map[string]interface{}{})
}

func (f *rollingUpdateFixture) podNames(t *testing.T) []string {
	t.Helper()
	var podList corev1.PodList
	require.NoError(t, f.client.List(context.Background(), &podList, client.InNamespace(testNamespace)))
	var names []string
	for i := range podList.Items {
		names = append(names, podList.Items[i].Name)
	}
	return names
}

func (f *rollingUpdateFixture) events() []string {
	var events []string
	for {
		select {
		case e := <-f.recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func requireRequeue(t *testing.T, err error, reasonContains string) {
	t.Helper()
	var requeue *subcontrollers.RequeueError
	require.True(t, errors.As(err, &requeue), "expected a RequeueError, got %v", err)
	assert.Contains(t, requeue.Reason, reasonContains)
}

func TestLeaderAwareRollingUpdate_NothingToDoWhenEveryPodIsUpdated(t *testing.T) {
	f := newRollingUpdateFixture(t, &fakeFrontendClient{},
		testStatefulSet(3),
		testPod(0, newRevision, true), testPod(1, newRevision, true), testPod(2, newRevision, true))

	require.NoError(t, f.reconcile(t))
	assert.Len(t, f.podNames(t), 3)
	assert.Empty(t, f.events())
}

func TestLeaderAwareRollingUpdate_WaitsForStatefulSetToBeObserved(t *testing.T) {
	sts := testStatefulSet(3)
	sts.Status.ObservedGeneration = 1
	f := newRollingUpdateFixture(t, &fakeFrontendClient{}, sts,
		testPod(0, oldRevision, true), testPod(1, oldRevision, true), testPod(2, oldRevision, true))

	requireRequeue(t, f.reconcile(t), "observe the FE statefulset")
	assert.Len(t, f.podNames(t), 3)
}

func TestLeaderAwareRollingUpdate_ReplacesFollowersFirstAndLeaderLast(t *testing.T) {
	frontends := &fakeFrontendClient{frontends: []Frontend{
		frontend(0, FrontendRoleFollower),
		frontend(1, FrontendRoleFollower),
		frontend(2, FrontendRoleLeader),
	}}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(3),
		testPod(0, oldRevision, true), testPod(1, oldRevision, true), testPod(2, oldRevision, true))

	// The leader runs on the highest ordinal, which the statefulset controller would restart first.
	// The follower with the highest ordinal goes first instead.
	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(1)+" to be replaced")
	assert.ElementsMatch(t, []string{fePodName(0), fePodName(2)}, f.podNames(t))
	assert.Empty(t, frontends.transferredTo)

	// Nothing happens while the replacement pod is missing or not Ready.
	requireRequeue(t, f.reconcile(t), "waiting for 3 FE pods, found 2")
	require.NoError(t, f.client.Create(context.Background(), testPod(1, newRevision, false)))
	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(1)+" to be ready")
	assert.Len(t, f.podNames(t), 3)

	// Ready is not enough: the leader has to see the FE alive again.
	pod1 := testPod(1, newRevision, true)
	require.NoError(t, f.client.Delete(context.Background(), pod1))
	require.NoError(t, f.client.Create(context.Background(), pod1))
	frontends.frontends[1].Alive = false
	requireRequeue(t, f.reconcile(t), "waiting for the FE of pod "+fePodName(1)+" to be alive")
	assert.Len(t, f.podNames(t), 3)

	// Then the remaining follower is replaced.
	frontends.frontends[1].Alive = true
	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(0)+" to be replaced")
	assert.ElementsMatch(t, []string{fePodName(1), fePodName(2)}, f.podNames(t))
	require.NoError(t, f.client.Create(context.Background(), testPod(0, newRevision, true)))

	// Finally the leader: leadership goes to the updated follower with the highest ordinal before the delete.
	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(2)+" to be replaced")
	assert.ElementsMatch(t, []string{fePodName(0), fePodName(1)}, f.podNames(t))
	require.Len(t, frontends.transferredTo, 1)
	assert.Equal(t, feHost(1), frontends.transferredTo[0].Host)

	// Once the leader pod is back at the new revision, the update is over.
	require.NoError(t, f.client.Create(context.Background(), testPod(2, newRevision, true)))
	frontends.frontends = []Frontend{
		frontend(0, FrontendRoleFollower),
		frontend(1, FrontendRoleLeader),
		frontend(2, FrontendRoleFollower),
	}
	require.NoError(t, f.reconcile(t))
	assert.Len(t, f.podNames(t), 3)

	events := f.events()
	require.Len(t, events, 4)
	assert.Contains(t, events[0], "deleted pod "+fePodName(1)+" (FOLLOWER)")
	assert.Contains(t, events[1], "deleted pod "+fePodName(0)+" (FOLLOWER)")
	assert.Contains(t, events[2], "transferred the leader role from pod "+fePodName(2)+" to pod "+fePodName(1))
	assert.Contains(t, events[3], "deleted pod "+fePodName(2)+" (LEADER)")
}

func TestLeaderAwareRollingUpdate_RestartsLeaderWhenTransferIsNotSupported(t *testing.T) {
	frontends := &fakeFrontendClient{
		frontends: []Frontend{
			frontend(0, FrontendRoleLeader),
			frontend(1, FrontendRoleFollower),
			frontend(2, FrontendRoleFollower),
		},
		transferErr: errors.New("Getting syntax error at line 1, column 13"),
	}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(3),
		testPod(0, oldRevision, true), testPod(1, newRevision, true), testPod(2, newRevision, true))

	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(0)+" to be replaced")
	assert.ElementsMatch(t, []string{fePodName(1), fePodName(2)}, f.podNames(t))
	require.Len(t, frontends.transferredTo, 1)
	assert.Equal(t, feHost(2), frontends.transferredTo[0].Host)

	events := f.events()
	require.Len(t, events, 2)
	assert.Contains(t, events[0], "a leader election will follow")
	assert.Contains(t, events[0], "syntax error")
	assert.Contains(t, events[1], "deleted pod "+fePodName(0)+" (LEADER)")
}

func TestLeaderAwareRollingUpdate_SingleFeHasNoTransferTarget(t *testing.T) {
	frontends := &fakeFrontendClient{frontends: []Frontend{frontend(0, FrontendRoleLeader)}}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(1), testPod(0, oldRevision, true))
	f.src = testCluster(1)

	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(0)+" to be replaced")
	assert.Empty(t, f.podNames(t))
	assert.Empty(t, frontends.transferredTo)
	events := f.events()
	require.Len(t, events, 2)
	assert.Contains(t, events[0], "no updated follower FE to transfer the leader role to")
}

func TestLeaderAwareRollingUpdate_PausesWhenShowFrontendsFails(t *testing.T) {
	frontends := &fakeFrontendClient{showErr: errors.New("Error 1045: Access denied for user 'root'")}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(3),
		testPod(0, oldRevision, true), testPod(1, oldRevision, true), testPod(2, oldRevision, true))

	err := f.reconcile(t)
	requireRequeue(t, err, "SHOW FRONTENDS failed")
	var requeue *subcontrollers.RequeueError
	require.True(t, errors.As(err, &requeue))
	assert.Equal(t, leaderAwareRollingUpdateRetryInterval, requeue.After)
	assert.Len(t, f.podNames(t), 3)

	events := f.events()
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], corev1.EventTypeWarning), events[0])
	assert.Contains(t, events[0], "MYSQL_PWD")
}

func TestLeaderAwareRollingUpdate_WaitsForLeaderElection(t *testing.T) {
	frontends := &fakeFrontendClient{frontends: []Frontend{
		frontend(0, FrontendRoleFollower),
		frontend(1, FrontendRoleFollower),
		frontend(2, FrontendRoleFollower),
	}}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(3),
		testPod(0, oldRevision, true), testPod(1, newRevision, true), testPod(2, newRevision, true))

	requireRequeue(t, f.reconcile(t), "waiting for a leader FE to be elected")
	assert.Len(t, f.podNames(t), 3)
}

func TestLeaderAwareRollingUpdate_MatchesFrontendsByPodIP(t *testing.T) {
	// HOST_TYPE=IP makes FE report pod IPs instead of FQDNs.
	frontends := &fakeFrontendClient{frontends: []Frontend{
		{Host: "10.0.0.1", EditLogPort: "9010", Role: FrontendRoleLeader, Alive: true},
		{Host: "10.0.0.2", EditLogPort: "9010", Role: FrontendRoleFollower, Alive: true},
		{Host: "10.0.0.3", EditLogPort: "9010", Role: FrontendRoleFollower, Alive: true},
	}}
	f := newRollingUpdateFixture(t, frontends,
		testStatefulSet(3),
		testPod(0, oldRevision, true), testPod(1, oldRevision, true), testPod(2, oldRevision, true))

	requireRequeue(t, f.reconcile(t), "waiting for pod "+fePodName(2)+" to be replaced")
	assert.ElementsMatch(t, []string{fePodName(0), fePodName(1)}, f.podNames(t))
}

func TestPodOrdinal(t *testing.T) {
	ordinal, err := podOrdinal("kube-starrocks-fe-10")
	require.NoError(t, err)
	assert.Equal(t, 10, ordinal)

	_, err = podOrdinal("kube-starrocks-fe-x")
	assert.Error(t, err)
	_, err = podOrdinal("nodash")
	assert.Error(t, err)
}
