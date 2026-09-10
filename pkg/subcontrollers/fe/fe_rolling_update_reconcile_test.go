package fe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	srapi "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	rutils "github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/resource_utils"
)

// The leader aware rolling update switches the FE statefulset to OnDelete and, while the update is not finished,
// makes the reconciler come back on its own instead of failing the cluster.
func TestStarRocksClusterReconciler_LeaderAwareRollingUpdateRequeues(t *testing.T) {
	src := &srapi.StarRocksCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "starrockscluster-sample", Namespace: "default"},
		Spec: srapi.StarRocksClusterSpec{
			StarRocksFeSpec: &srapi.StarRocksFeSpec{
				StarRocksComponentSpec: srapi.StarRocksComponentSpec{
					StarRocksLoadSpec: srapi.StarRocksLoadSpec{
						Replicas: rutils.GetInt32Pointer(3),
						Image:    "starrocks/fe-ubuntu:3.5.4",
					},
				},
				LeaderAwareRollingUpdate: true,
			},
		},
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      src.Name + "-" + srapi.DEFAULT_FE,
		Namespace: "default",
	}}

	r := newStarRocksClusterController(src, sts)
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "starrockscluster-sample",
	}})
	require.NoError(t, err)
	assert.Greater(t, res.RequeueAfter.Seconds(), 0.0)

	var actual appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: sts.Name}, &actual))
	assert.Equal(t, appsv1.OnDeleteStatefulSetStrategyType, actual.Spec.UpdateStrategy.Type)

	var cluster srapi.StarRocksCluster
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: src.Name}, &cluster))
	assert.NotEqual(t, srapi.ClusterFailed, cluster.Status.Phase)
	require.NotNil(t, cluster.Status.StarRocksFeStatus)
	assert.Equal(t, srapi.ComponentReconciling, cluster.Status.StarRocksFeStatus.Phase)

	// once the flag is removed the statefulset goes back to the strategy of the spec
	cluster.Spec.StarRocksFeSpec.LeaderAwareRollingUpdate = false
	require.NoError(t, r.Update(context.Background(), &cluster))
	res, err = r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "starrockscluster-sample",
	}})
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: sts.Name}, &actual))
	assert.Equal(t, appsv1.RollingUpdateStatefulSetStrategyType, actual.Spec.UpdateStrategy.Type)
}
