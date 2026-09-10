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

// Package sqlexec runs SQL statements against the FE of a StarRocks cluster over the MySQL protocol.
// It holds the connection details and the SSL handling that every component controller shares;
// component specific statements (SHOW COMPUTE NODES, SHOW FRONTENDS, ...) live next to the controller
// that issues them.
package sqlexec

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

// Executor is used to execute sql statements against FE.
type Executor struct {
	RootPassword       string
	FeServiceName      string
	FeServiceNamespace string
	FeServicePort      string

	// SSLMode decides whether the connection to FE negotiates TLS; see the SSLMode* constants.
	// It is captured per executor rather than read from the flag at connect time so that tests can
	// exercise each mode without mutating process-wide state.
	SSLMode string
}

// DSN builds the go-sql-driver DSN for the FE connection. Both ExecuteContext and QueryContext go
// through it so the SSL mode can never be honored by one path and skipped by the other.
func (executor *Executor) DSN() string {
	return fmt.Sprintf("root:%s@tcp(%s.%s:%s)/%s",
		executor.RootPassword, executor.FeServiceName, executor.FeServiceNamespace,
		executor.FeServicePort, sslModeDSNSuffix(executor.SSLMode))
}

// ExecuteContext sql statements. Every time a SQL statement needs to be executed, a new sql.DB instance will be created.
// This is because SQL statements are executed infrequently.
func (executor *Executor) ExecuteContext(ctx context.Context, db *sql.DB, statement string) error {
	var err error
	if db == nil {
		db, err = sql.Open("mysql", executor.DSN())
		if err != nil {
			return err
		}
		defer db.Close()
	}

	_, err = db.ExecContext(ctx, statement)
	if err != nil {
		return err
	}

	return nil
}

func (executor *Executor) QueryContext(ctx context.Context, db *sql.DB, statements string) (*sql.Rows, error) {
	var err error
	if db == nil {
		db, err = sql.Open("mysql", executor.DSN())
		if err != nil {
			return nil, err
		}
		defer db.Close()
	}

	rows, err := db.QueryContext(ctx, statements)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// QueryRows runs a statement and returns every row as a column name to value map. FE returns all
// columns of SHOW statements as strings, so the map form is enough and it keeps callers independent of
// the column order, which differs between FE versions.
func (executor *Executor) QueryRows(ctx context.Context, db *sql.DB, statement string) ([]map[string]string, error) {
	var err error
	if db == nil {
		db, err = sql.Open("mysql", executor.DSN())
		if err != nil {
			return nil, err
		}
		defer db.Close()
	}

	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	for rows.Next() {
		// Note: all data types of fields are sql.RawBytes([]byte)
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err = rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]string, len(columns))
		for i, col := range columns {
			switch v := values[i].(type) {
			case nil:
				row[col] = ""
			case []byte:
				row[col] = string(v)
			default:
				row[col] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
