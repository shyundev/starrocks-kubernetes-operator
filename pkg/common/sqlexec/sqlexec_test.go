package sqlexec

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorDSN(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want string
	}{
		{
			name: "disabled keeps the plaintext DSN unchanged",
			mode: SSLModeDisabled,
			want: "root:123456@tcp(fe.default:9030)/",
		},
		{
			name: "preferred appends the opportunistic tls parameter",
			mode: SSLModePreferred,
			want: "root:123456@tcp(fe.default:9030)/?tls=preferred",
		},
		{
			name: "required appends the mandatory tls parameter",
			mode: SSLModeRequired,
			want: "root:123456@tcp(fe.default:9030)/?tls=skip-verify",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &Executor{
				RootPassword:       "123456",
				FeServiceName:      "fe",
				FeServiceNamespace: "default",
				FeServicePort:      "9030",
				SSLMode:            tt.mode,
			}
			assert.Equal(t, tt.want, executor.DSN())
		})
	}
}

func TestExecutor_ExecuteContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("drop warehouse test").WillReturnResult(sqlmock.NewResult(1, 1))

	executor := &Executor{RootPassword: "root", FeServiceName: "localhost", FeServicePort: "3306"}
	assert.NoError(t, executor.ExecuteContext(context.Background(), db, "drop warehouse test"))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExecutor_QueryRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SHOW FRONTENDS").WillReturnRows(
		sqlmock.NewRows([]string{"Name", "Role", "Alive", "ErrMsg"}).
			AddRow([]byte("fe-0"), []byte("LEADER"), []byte("true"), nil).
			AddRow([]byte("fe-1"), []byte("FOLLOWER"), []byte("false"), []byte("timeout")),
	)

	executor := &Executor{RootPassword: "root", FeServiceName: "localhost", FeServicePort: "3306"}
	rows, err := executor.QueryRows(context.Background(), db, "SHOW FRONTENDS")
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{
		{"Name": "fe-0", "Role": "LEADER", "Alive": "true", "ErrMsg": ""},
		{"Name": "fe-1", "Role": "FOLLOWER", "Alive": "false", "ErrMsg": "timeout"},
	}, rows)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExecutor_QueryRowsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SHOW FRONTENDS").WillReturnError(assert.AnError)

	executor := &Executor{RootPassword: "root", FeServiceName: "localhost", FeServicePort: "3306"}
	_, err = executor.QueryRows(context.Background(), db, "SHOW FRONTENDS")
	assert.Error(t, err)
}
