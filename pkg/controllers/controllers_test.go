package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/fake"
)

func TestSetupClusterReconciler(t *testing.T) {
	type args struct {
		mgr controllerruntime.Manager
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test setup cluster reconciler",
			args: args{
				mgr: func() controllerruntime.Manager {
					env := fake.NewEnvironment(fake.WithClusterCRD())
					defer func() {
						err := env.Stop()
						assert.Nil(t, err)
					}()
					return fake.NewManager(env)
				}(),
			},
			wantErr: false,
		},
	}

	v1.Register()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SetupClusterReconciler(tt.args.mgr, ""); (err != nil) != tt.wantErr {
				t.Errorf("SetupClusterReconciler() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSetupWarehouseReconciler(t *testing.T) {
	env1 := fake.NewEnvironment(fake.WithClusterCRD())
	env2 := fake.NewEnvironment(fake.WithClusterCRD(), fake.WithWarehouseCRD())
	defer func() {
		err := env1.Stop()
		assert.Nil(t, err)
		err = env2.Stop()
		assert.Nil(t, err)
	}()

	type args struct {
		mgr       controllerruntime.Manager
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test setup warehouse reconciler with no warehouse CRD",
			args: args{
				mgr: func() controllerruntime.Manager {
					env1 = fake.NewEnvironment(fake.WithClusterCRD())
					return fake.NewManager(env1)
				}(),
				namespace: "",
			},
			wantErr: false,
		},
		{
			name: "test setup warehouse reconciler with warehouse CRD",
			args: args{
				mgr: func() controllerruntime.Manager {
					return fake.NewManager(env2)
				}(),
				namespace: "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SetupWarehouseReconciler(tt.args.mgr, tt.args.namespace, ""); (err != nil) != tt.wantErr {
				t.Errorf("SetupWarehouseReconciler() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type Reader struct {
	hasWarehouseCRD bool
}

func (r Reader) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return nil
}

func (r Reader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	if r.hasWarehouseCRD {
		return nil
	}
	return &meta.NoKindMatchError{}
}

var _ client.Reader = &Reader{}

func TestClusterOfFEService(t *testing.T) {
	slice := func(serviceName string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "ns",
			Labels: map[string]string{discoveryv1.LabelServiceName: serviceName}}}
	}
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "kube-starrocks"}}}
	assert.Equal(t, want, clusterOfFEService(slice("kube-starrocks-fe-service")))
	assert.Equal(t, want, clusterOfFEService(&corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-starrocks-fe-service", Namespace: "ns"}}))
	assert.Empty(t, clusterOfFEService(slice("kube-starrocks-cn-service")))
	assert.Empty(t, clusterOfFEService(slice("-fe-service")))
	assert.Empty(t, clusterOfFEService(&discoveryv1.EndpointSlice{}))
}
