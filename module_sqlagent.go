package sqlagent

import (
	"context"
	"database/sql"
	"github.com/RivenZoo/dsncfg"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	sq "gopkg.in/Masterminds/squirrel.v1"
)

var (
	defaultAgent *SqlAgent
)

// Close SqlAgent inited by module init method.
func Close() error {
	return defaultAgent.Close()
}

// DB return sqlx.DB held by module SqlAgent.
func DB() *sqlx.DB {
	return defaultAgent.DB()
}

func Transaction(ctx context.Context, opt *sql.TxOptions, fn func(tx *sqlx.Tx) error) error {
	return defaultAgent.Transaction(ctx, opt, fn)
}

// InsertBuilder return squirrel.InsertBuilder for table into
// into: insert table name
func InsertBuilder(into string) sq.InsertBuilder {
	return defaultAgent.InsertBuilder(into)
}

func UpdateBuilder(table string) sq.UpdateBuilder {
	return defaultAgent.UpdateBuilder(table)
}

func DeleteBuilder(table string) sq.DeleteBuilder {
	return defaultAgent.DeleteBuilder(table)
}

func SelectBuilder(columns ...string) sq.SelectBuilder {
	return defaultAgent.SelectBuilder(columns...)
}

func InsertModelBuilder(into string, model interface{}, ignoreColumns ...string) sq.InsertBuilder {
	return defaultAgent.InsertModelBuilder(into, model, ignoreColumns...)
}

func SetUpdateColumns(updateBuilder sq.UpdateBuilder, model interface{}, ignoreColumns ...string) sq.UpdateBuilder {
	return defaultAgent.SetUpdateColumns(updateBuilder, model, ignoreColumns...)
}

// SetDBMapper set mapper for module sqlagent
func SetDBMapper(mapper *reflectx.Mapper) {
	defaultAgent.SetDBMapper(mapper)
}

// SetConnectionConfig set conenction for module sqlagent.
func SetConnectionConfig(cfg dsncfg.ConnectionConfig) {
	defaultAgent.SetConnectionConfig(cfg)
}

// ModelColumns use module sqlagent to extract model columns.
func ModelColumns(model interface{}, ignoreColumns ...string) []string {
	return defaultAgent.ModelColumns(model, ignoreColumns...)
}

// ExecContext exec sql built by sq.InsertBuilder/sq.UpdateBuilder/sq.DeleteBuilder and return result.
// builder: sq.InsertBuilder, sq.UpdateBuilder or sq.DeleteBuilder
func ExecContext(ctx context.Context, builder sq.Sqlizer) (sql.Result, error) {
	return defaultAgent.ExecContext(ctx, builder)
}

// GetContext get one record by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func GetContext(ctx context.Context, builder sq.Sqlizer, dest interface{}) error {
	return defaultAgent.GetContext(ctx, builder, dest)
}

// SelectContext get one or multi records by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func SelectContext(ctx context.Context, builder sq.Sqlizer, dest interface{}) error {
	return defaultAgent.SelectContext(ctx, builder, dest)
}
