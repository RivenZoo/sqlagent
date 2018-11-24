package sqlagent

import (
	"context"
	"database/sql"
	"errors"
	"github.com/RivenZoo/dsncfg"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	sq "gopkg.in/Masterminds/squirrel.v1"
	"reflect"
)

var (
	errorWrongConfig      = errors.New("database config error")
	errorWrongArgs        = errors.New("func args error")
	errorNotFoundDBConfig = errors.New("not found database config error")
)

type SqlAgent struct {
	db *sqlx.DB
}

func NewSqlAgent(cfg *dsncfg.Database) (*SqlAgent, error) {
	if cfg == nil {
		return nil, errorWrongConfig
	}
	err := cfg.Init()
	if err != nil {
		return nil, err
	}
	dsn := cfg.DSN()
	agent := &SqlAgent{}

	db, err := sqlx.ConnectContext(context.Background(), driverName(cfg), dsn)
	if err != nil {
		return nil, err
	}
	agent.db = db
	if cfg.Type == dsncfg.Postgresql {
		sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	}
	return agent, nil
}

func driverName(cfg *dsncfg.Database) string {
	switch cfg.Type {
	case dsncfg.MySql:
		return "mysql"
	case dsncfg.Postgresql:
		return "postgres"
	case dsncfg.Sqlite:
		return "sqlite3"
	}
	return ""
}

func (a *SqlAgent) Close() error {
	return a.db.Close()
}

func (a *SqlAgent) DB() *sqlx.DB {
	return a.db
}

func (a *SqlAgent) Transaction(ctx context.Context, opt *sql.TxOptions, fn func(tx *sqlx.Tx) error) error {
	tx, err := a.db.BeginTxx(ctx, opt)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// InsertBuilder return squirrel.InsertBuilder for table into
// into: insert table name
func (a *SqlAgent) InsertBuilder(into string) sq.InsertBuilder {
	return sq.Insert(into)
}

func (a *SqlAgent) UpdateBuilder(table string) sq.UpdateBuilder {
	return sq.Update(table)
}

func (a *SqlAgent) DeleteBuilder(table string) sq.DeleteBuilder {
	return sq.Delete(table)
}

func (a *SqlAgent) SelectBuilder(columns ...string) sq.SelectBuilder {
	return sq.Select(columns...)
}

// InsertModelBuilder use name and value of model feild to build insert sql.
// ignoreColumns should be the same with column name that is converted by sqlx.DB.Mapper.
func (a *SqlAgent) InsertModelBuilder(into string, model interface{}, ignoreColumns ...string) sq.InsertBuilder {
	fieldMap := a.db.Mapper.TypeMap(reflect.TypeOf(model))
	valueMap := a.db.Mapper.FieldMap(reflect.Indirect(reflect.ValueOf(model)))

	builder := sq.Insert(into)

	var params []interface{}
	var columnNames []string

	for _, v := range fieldMap.Index {
		name := v.Name
		if isIgnoreFields(name, ignoreColumns) {
			continue
		}
		if data, ok := valueMap[name]; ok {
			columnNames = append(columnNames, name)
			params = append(params, data.Interface())
		}
	}
	if len(columnNames) > 0 {
		builder = builder.Columns(columnNames...).Values(params...)
	}
	return builder
}

// SetUpdateColumns use name and value of model feild to build update sql.
// ignoreColumns should be the same with column name that is converted by sqlx.DB.Mapper.
func (a *SqlAgent) SetUpdateColumns(updateBuilder sq.UpdateBuilder, model interface{}, ignoreColumns ...string) sq.UpdateBuilder {
	fieldMap := a.db.Mapper.TypeMap(reflect.TypeOf(model))
	valueMap := a.db.Mapper.FieldMap(reflect.Indirect(reflect.ValueOf(model)))
	clauses := make(map[string]interface{})

	for _, v := range fieldMap.Index {
		name := v.Name
		if isIgnoreFields(name, ignoreColumns) {
			continue
		}
		if data, ok := valueMap[name]; ok {
			clauses[name] = data.Interface()
		}
	}
	return updateBuilder.SetMap(clauses)
}

// ExecContext exec sql built by sq.InsertBuilder/sq.UpdateBuilder/sq.DeleteBuilder and return result.
// builder: sq.InsertBuilder, sq.UpdateBuilder or sq.DeleteBuilder
func (a *SqlAgent) ExecContext(ctx context.Context, builder sq.Sqlizer) (sql.Result, error) {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	return a.db.ExecContext(ctx, sqlStr, args...)
}

// GetContext get one record by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func (a *SqlAgent) GetContext(ctx context.Context, builder sq.Sqlizer, dest interface{}) error {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	return a.db.GetContext(ctx, dest, sqlStr, args...)
}

// SelectContext get one or multi records by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func (a *SqlAgent) SelectContext(ctx context.Context, builder sq.Sqlizer, dest interface{}) error {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	return a.db.SelectContext(ctx, dest, sqlStr, args...)
}

// SetDBMapper set mapper to sqlx.DB.Mapper.
// Default mapper use tag `db`, if no tags it will use lower case field name as column name.
func (a *SqlAgent) SetDBMapper(mapper *reflectx.Mapper) {
	a.db.Mapper = mapper
}

// ModelColumns use sqlx.DB.Mapper to extract model table columns name.
// Columns in ignoreColumns will be ignored.
// ignoreColumns should be the same with column name that is converted by sqlx.DB.Mapper.
func (a *SqlAgent) ModelColumns(model interface{}, ignoreColumns ...string) []string {
	fieldMap := a.db.Mapper.TypeMap(reflect.TypeOf(model))

	columns := make([]string, 0)
	for _, v := range fieldMap.Index {
		name := v.Name
		if isIgnoreFields(name, ignoreColumns) {
			continue
		}
		columns = append(columns, name)
	}
	return columns
}

// TxExecContext exec sql built by sq.InsertBuilder/sq.UpdateBuilder/sq.DeleteBuilder and return result.
// builder: sq.InsertBuilder, sq.UpdateBuilder or sq.DeleteBuilder
func TxExecContext(ctx context.Context, tx *sqlx.Tx, builder sq.Sqlizer) (sql.Result, error) {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, sqlStr, args...)
}

// TxGetContext get one record by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func TxGetContext(ctx context.Context, tx *sqlx.Tx, builder sq.Sqlizer, dest interface{}) error {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	return tx.GetContext(ctx, dest, sqlStr, args...)
}

// TxSelectContext get one or multi records by sql built by sq.SelectBuilder and scan to dest.
// builder: sq.SelectBuilder
func TxSelectContext(ctx context.Context, tx *sqlx.Tx, builder sq.Sqlizer, dest interface{}) error {
	sqlStr, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	return tx.SelectContext(ctx, dest, sqlStr, args...)
}

func isIgnoreFields(name string, ignore []string) bool {
	for _, nameIgnore := range ignore {
		if nameIgnore == name {
			return true
		}
	}
	return false
}
