package sqlagent

import (
	"github.com/jmoiron/sqlx"
	"github.com/RivenZoo/dsncfg"
	"errors"
	"context"
	"database/sql"
	sq "gopkg.in/Masterminds/squirrel.v1"
)

var (
	errorWrongConfig = errors.New("database config error")
	errorWrongArgs   = errors.New("func args error")
)

type SqlAgent struct {
	db     *sqlx.DB
	dbType string
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
	agent := &SqlAgent{
		dbType: cfg.Type,
	}

	db, err := sqlx.ConnectContext(context.Background(), driverName(cfg), dsn)
	if err != nil {
		return nil, err
	}
	agent.db = db
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
	if a.dbType == dsncfg.Postgresql {
		return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Insert(into)
	}
	return sq.StatementBuilder.Insert(into)
}

func (a *SqlAgent) UpdateBuilder(table string) sq.UpdateBuilder {
	if a.dbType == dsncfg.Postgresql {
		return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Update(table)
	}
	return sq.StatementBuilder.Update(table)
}

func (a *SqlAgent) DeleteBuilder(table string) sq.DeleteBuilder {
	if a.dbType == dsncfg.Postgresql {
		return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Delete(table)
	}
	return sq.StatementBuilder.Delete(table)
}

func (a *SqlAgent) SelectBuilder(columns ...string) sq.SelectBuilder {
	if a.dbType == dsncfg.Postgresql {
		return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Select(columns...)
	}
	return sq.StatementBuilder.Select(columns...)
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
