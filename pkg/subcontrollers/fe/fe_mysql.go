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
	"database/sql"
	"fmt"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/StarRocks/starrocks-kubernetes-operator/cmd/config"
	srapi "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	rutils "github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/resource_utils"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/common/sqlexec"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils"
	"github.com/StarRocks/starrocks-kubernetes-operator/pkg/k8sutils/templates/service"
)

const (
	ShowFrontendsStatement = "SHOW FRONTENDS"

	FrontendRoleLeader   = "LEADER"
	FrontendRoleFollower = "FOLLOWER"
)

// Frontend is one row of SHOW FRONTENDS, reduced to the columns the rolling update needs.
type Frontend struct {
	// Host is the IP column: the FQDN of the pod with the default HOST_TYPE=FQDN, otherwise its IP.
	Host        string
	EditLogPort string
	// Role is LEADER, FOLLOWER, OBSERVER or UNKNOWN.
	Role  string
	Alive bool
}

// FrontendClient is what the rolling update needs from FE. It is an interface so that tests can drive
// the update without a MySQL server.
type FrontendClient interface {
	ShowFrontends(ctx context.Context) ([]Frontend, error)
	// TransferLeader asks the current leader to hand leadership over to target, which must be an
	// alive follower.
	TransferLeader(ctx context.Context, target Frontend) error
}

// SQLFrontendClient talks to FE through the FE service with the root account, like CN scale-in does.
type SQLFrontendClient struct {
	Executor *sqlexec.Executor
	// DB is nil in production, where every statement opens its own connection. Tests inject a mock.
	DB *sql.DB
}

var _ FrontendClient = &SQLFrontendClient{}

// NewSQLFrontendClient builds the client from the FE spec: the root password comes from the MYSQL_PWD
// environment variable of feEnvVars when there is one, the endpoint is the FE service and the query port
// of the FE config.
func NewSQLFrontendClient(ctx context.Context, k8sClient client.Client, src *srapi.StarRocksCluster,
	feConfig map[string]interface{}) (*SQLFrontendClient, error) {
	feSpec := src.Spec.StarRocksFeSpec
	rootPassword := ""
	for _, envVar := range feSpec.FeEnvVars {
		if envVar.Name != "MYSQL_PWD" {
			continue
		}
		value, err := k8sutils.GetEnvVarValue(ctx, k8sClient, src.Namespace, envVar)
		if err != nil {
			return nil, fmt.Errorf("get MYSQL_PWD from feEnvVars: %w", err)
		}
		rootPassword = value
	}

	return &SQLFrontendClient{
		Executor: &sqlexec.Executor{
			RootPassword:       rootPassword,
			FeServiceName:      service.ExternalServiceName(src.Name, feSpec),
			FeServiceNamespace: src.Namespace,
			FeServicePort:      strconv.FormatInt(int64(rutils.GetPort(feConfig, rutils.QUERY_PORT)), 10),
			SSLMode:            config.FeSslMode,
		},
	}, nil
}

func (c *SQLFrontendClient) ShowFrontends(ctx context.Context) ([]Frontend, error) {
	rows, err := c.Executor.QueryRows(ctx, c.DB, ShowFrontendsStatement)
	if err != nil {
		return nil, err
	}

	frontends := make([]Frontend, 0, len(rows))
	for _, row := range rows {
		frontends = append(frontends, Frontend{
			Host:        row["IP"],
			EditLogPort: row["EditLogPort"],
			Role:        row["Role"],
			Alive:       row["Alive"] == "true",
		})
	}
	return frontends, nil
}

func (c *SQLFrontendClient) TransferLeader(ctx context.Context, target Frontend) error {
	return c.Executor.ExecuteContext(ctx, c.DB, TransferLeaderStatement(target))
}

// TransferLeaderStatement addresses the target the same way ADD FOLLOWER does, by "host:edit_log_port".
func TransferLeaderStatement(target Frontend) string {
	return fmt.Sprintf("ALTER SYSTEM TRANSFER LEADER TO \"%s:%s\"", target.Host, target.EditLogPort)
}
