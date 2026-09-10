package statefulset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newStatefulSet(strategy appsv1.StatefulSetUpdateStrategyType, replicas int32,
	status appsv1.StatefulSetStatus) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: strategy},
		},
		Status: status,
	}
}

func TestStatus_OnDelete(t *testing.T) {
	tests := []struct {
		name     string
		sts      *appsv1.StatefulSet
		wantDone bool
		wantMsg  string
	}{
		{
			name: "spec not observed yet",
			sts: newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3, appsv1.StatefulSetStatus{
				ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3,
				CurrentRevision: "v1", UpdateRevision: "v1",
			}),
			wantDone: false,
			wantMsg:  "Waiting for statefulset spec update to be observed",
		},
		{
			name: "a pod is being replaced",
			sts: newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 2, UpdatedReplicas: 1,
				CurrentRevision: "v1", UpdateRevision: "v2",
			}),
			wantDone: false,
			wantMsg:  "Waiting for 1 pods to be ready",
		},
		{
			name: "pods still run the old revision",
			sts: newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 3, UpdatedReplicas: 1,
				CurrentRevision: "v1", UpdateRevision: "v2",
			}),
			wantDone: false,
			wantMsg:  "waiting for 2 pods to be replaced at revision v2",
		},
		{
			// The statefulset controller never sets currentRevision for OnDelete, so the mismatch must not
			// keep the component reconciling forever.
			name: "every pod replaced although currentRevision lags behind",
			sts: newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, ReadyReplicas: 3, UpdatedReplicas: 3,
				CurrentRevision: "v1", UpdateRevision: "v2",
			}),
			wantDone: true,
			wantMsg:  "statefulset OnDelete update complete 3 pods at revision v2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, done, err := Status(tt.sts)
			require.NoError(t, err)
			assert.Equal(t, tt.wantDone, done)
			assert.Equal(t, tt.wantMsg, msg)
		})
	}
}

func TestStatus_RollingUpdateUnchanged(t *testing.T) {
	sts := newStatefulSet(appsv1.RollingUpdateStatefulSetStrategyType, 3, appsv1.StatefulSetStatus{
		ObservedGeneration: 2, ReadyReplicas: 3, UpdatedReplicas: 1, CurrentRevision: "v1", UpdateRevision: "v2",
	})
	msg, done, err := Status(sts)
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, "waiting for statefulset rolling update to complete 1 pods at revision v2", msg)

	sts.Status.CurrentRevision = "v2"
	sts.Status.CurrentReplicas = 3
	msg, done, err = Status(sts)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, "statefulset rolling update complete 3 pods at revision v2", msg)
}

func TestStatus_UnknownStrategy(t *testing.T) {
	sts := newStatefulSet("Unknown", 1, appsv1.StatefulSetStatus{})
	_, _, err := Status(sts)
	assert.Error(t, err)
}

func TestRevisionRolledOut(t *testing.T) {
	assert.True(t, RevisionRolledOut(newStatefulSet(appsv1.RollingUpdateStatefulSetStrategyType, 3,
		appsv1.StatefulSetStatus{CurrentRevision: "v2", UpdateRevision: "v2"})))
	assert.False(t, RevisionRolledOut(newStatefulSet(appsv1.RollingUpdateStatefulSetStrategyType, 3,
		appsv1.StatefulSetStatus{CurrentRevision: "v1", UpdateRevision: "v2", UpdatedReplicas: 3})))
	assert.True(t, RevisionRolledOut(newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3,
		appsv1.StatefulSetStatus{CurrentRevision: "v1", UpdateRevision: "v2", UpdatedReplicas: 3})))
	assert.False(t, RevisionRolledOut(newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 3,
		appsv1.StatefulSetStatus{CurrentRevision: "v1", UpdateRevision: "v2", UpdatedReplicas: 2})))

	// replicas defaults to 1 when it is not set
	sts := newStatefulSet(appsv1.OnDeleteStatefulSetStrategyType, 1,
		appsv1.StatefulSetStatus{CurrentRevision: "v1", UpdateRevision: "v2", UpdatedReplicas: 1})
	sts.Spec.Replicas = nil
	assert.True(t, RevisionRolledOut(sts))
}
