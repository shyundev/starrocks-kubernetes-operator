package fe

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/StarRocks/starrocks-kubernetes-operator/cmd/config"
	srapi "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	rutils "github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/resource_utils"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/sqlexec"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/fake"
)

func TestNewSQLFrontendClient(t *testing.T) {
	srapi.Register()
	config.FeSslMode = sqlexec.SSLModePreferred
	src := testCluster(3)
	src.Spec.StarRocksFeSpec.FeEnvVars = []corev1.EnvVar{
		{Name: "TZ", Value: "UTC"},
		{Name: "MYSQL_PWD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "root-password"},
			Key:                  "password",
		}}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "root-password", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	feConfig := map[string]interface{}{rutils.QUERY_PORT: "9031"}

	frontendClient, err := NewSQLFrontendClient(context.Background(), fake.NewFakeClient(srapi.Scheme, secret), src, feConfig)
	require.NoError(t, err)
	assert.Equal(t, &sqlexec.Executor{
		RootPassword:       "s3cret",
		FeServiceName:      testClusterName + "-fe-service",
		FeServiceNamespace: testNamespace,
		FeServicePort:      "9031",
		SSLMode:            sqlexec.SSLModePreferred,
	}, frontendClient.Executor)

	// a missing secret is reported instead of silently connecting without a password
	_, err = NewSQLFrontendClient(context.Background(), fake.NewFakeClient(srapi.Scheme), src, feConfig)
	assert.Error(t, err)

	// no MYSQL_PWD means an empty root password, the default of a fresh cluster
	src.Spec.StarRocksFeSpec.FeEnvVars = nil
	frontendClient, err = NewSQLFrontendClient(context.Background(), fake.NewFakeClient(srapi.Scheme), src,
		map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, "", frontendClient.Executor.RootPassword)
	assert.Equal(t, "9030", frontendClient.Executor.FeServicePort, "the default query port applies without config")
}

func TestSQLFrontendClient_ShowFrontends(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery(ShowFrontendsStatement).WillReturnRows(
		sqlmock.NewRows([]string{"Name", "IP", "EditLogPort", "HttpPort", "QueryPort", "Role", "Alive", "ErrMsg"}).
			AddRow([]byte("fe_a"), []byte(feHost(0)), []byte("9010"), []byte("8030"), []byte("9030"),
				[]byte("LEADER"), []byte("true"), []byte("")).
			AddRow([]byte("fe_b"), []byte(feHost(1)), []byte("9010"), []byte("8030"), []byte("9030"),
				[]byte("FOLLOWER"), []byte("false"), []byte("java.net.SocketTimeoutException")),
	)

	frontendClient := &SQLFrontendClient{Executor: &sqlexec.Executor{}, DB: db}
	frontends, err := frontendClient.ShowFrontends(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []Frontend{
		{Host: feHost(0), EditLogPort: "9010", Role: FrontendRoleLeader, Alive: true},
		{Host: feHost(1), EditLogPort: "9010", Role: FrontendRoleFollower, Alive: false},
	}, frontends)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLFrontendClient_TransferLeader(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	target := Frontend{Host: feHost(1), EditLogPort: "9010", Role: FrontendRoleFollower, Alive: true}
	mock.ExpectExec(`ALTER SYSTEM TRANSFER LEADER TO "` + feHost(1) + `:9010"`).WillReturnResult(sqlmock.NewResult(0, 0))

	frontendClient := &SQLFrontendClient{Executor: &sqlexec.Executor{}, DB: db}
	assert.NoError(t, frontendClient.TransferLeader(context.Background(), target))
	assert.NoError(t, mock.ExpectationsWereMet())
}
