package sqlagent

import (
	"testing"
	"github.com/RivenZoo/dsncfg"
	"github.com/jmoiron/sqlx/reflectx"
	sq "gopkg.in/Masterminds/squirrel.v1"
	"strings"
	"context"
	_ "github.com/go-sql-driver/mysql"
)

type tableUser struct {
	ID   int64  `json:"id"` // primary key
	Name string `json:"name"`
	UID  int64  `json:"uid"`
}

func testExecSql(sa *SqlAgent, table string, t *testing.T) {
	userName := "testuser"
	uid := 1238278327

	// insert
	insertBuilder := sa.InsertBuilder(table).
		Columns("name", "uid").
		Values(userName, uid)
	_, err := sa.ExecContext(context.TODO(), insertBuilder)
	if err != nil {
		s, _, e := insertBuilder.ToSql()
		t.Fatalf("ExecContext error: %v, sql: %s,%v", err, s, e)
	}

	// select
	selectBuilder := sa.SelectBuilder("*").From(table).
		Where(sq.Eq{"name": userName})
	userRes := []*tableUser{}
	err = sa.SelectContext(context.TODO(), selectBuilder, &userRes)
	if err != nil {
		s, _, e := selectBuilder.ToSql()
		t.Fatalf("SelectContext error: %v, sql: %s,%v", err, s, e)
	}
	t.Logf("%v", userRes)

	// update
	oldUserName := userName
	userName = "newname"
	updateBuilder := sa.UpdateBuilder(table).Where(sq.Eq{"name": oldUserName}).
		Set("name", userName)
	res, err := sa.ExecContext(context.TODO(), updateBuilder)
	if err != nil {
		s, _, e := updateBuilder.ToSql()
		t.Fatalf("ExecContext error: %v, sql: %s,%v", err, s, e)
	}
	n, err := res.RowsAffected()
	if n != int64(1) {
		t.Fatalf("RowsAffected n: %d, error: %v", n, err)
	}
	
	// delete
	delBuilder := sa.DeleteBuilder(table).
		Where("name=?", userName)
	res, err = sa.ExecContext(context.TODO(), delBuilder)
	if err != nil {
		s, _, e := delBuilder.ToSql()
		t.Fatalf("ExecContext error: %v, sql: %s,%v", err, s, e)
	}
	n, err = res.RowsAffected()
	if n != int64(1) {
		t.Fatalf("RowsAffected n: %d, error: %v", n, err)
	}
}

func TestSqlAgent_ExecSql(t *testing.T) {
	// sql: create database test;
	//      CREATE USER 'test'@'localhost' IDENTIFIED BY 'passwd';
	//      GRANT ALL PRIVILEGES ON test.* TO 'test'@'localhost';
	userTable := "testuser"
	dbName := "test"
	user := "test"
	passwd := "passwd"
	testCfg := []dsncfg.Database{
		dsncfg.Database{
			Host:     "localhost",
			Port:     3306,
			Name:     dbName,
			Type:     "mysql",
			User:     user,
			Password: passwd,
		},
	}
	createSqls := []string{
		`CREATE TABLE ` + userTable + ` (id BIGINT AUTO_INCREMENT PRIMARY KEY ,
name varchar(64) default "" NOT NULL,
uid BIGINT default 0 NOT NULL)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
	}
	dropSql := []string{
		`DROP TABLE IF EXISTS ` + userTable + `;`,
	}
	for i := range testCfg {
		cfg := &testCfg[i]
		sa, err := NewSqlAgent(cfg)
		if err != nil {
			t.Fatalf("NewSqlAgent error: %v", err)
		}
		sa.DB().SetMaxIdleConns(2)
		sa.DB().SetMaxOpenConns(4)
		sa.DB().Mapper = reflectx.NewMapperFunc("json", strings.ToLower)

		func() {
			defer sa.Close()
			sa.DB().Exec(dropSql[i])
			_, err = sa.DB().Exec(createSqls[i])
			if err != nil {
				t.Fatalf("createSql error: %v", err)
			}
			testExecSql(sa, userTable, t)
			sa.DB().Exec(dropSql[i])
		}()
	}
}
