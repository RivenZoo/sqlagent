package sqlagent

import (
	"context"
	"github.com/RivenZoo/dsncfg"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/stretchr/testify/assert"
	sq "gopkg.in/Masterminds/squirrel.v1"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

type tableUser struct {
	ID   int64  `json:"id" db:"id"` // primary key
	Name string `json:"name" db:"name"`
	UID  int64  `json:"uid" db:"uid"`
}

func testExecSql(sa *SqlAgent, table string, t *testing.T) {
	userName := "testuser"
	uid := 1238278327

	insertNum := 2

	// insert
	insertBuilder := sa.InsertBuilder(table).
		Columns("name", "uid").
		Values(userName, uid)
	_, err := sa.ExecContext(context.TODO(), insertBuilder)
	if err != nil {
		s, _, e := insertBuilder.ToSql()
		t.Fatalf("ExecContext error: %v, sql: %s,%v", err, s, e)
	}

	insertBuilder = sa.InsertModelBuilder(table, tableUser{
		Name: userName,
		UID:  int64(uid + 1),
	}, "id")
	_, err = sa.ExecContext(context.TODO(), insertBuilder)
	if err != nil {
		s, _, e := insertBuilder.ToSql()
		t.Fatalf("ExecContext error: %v, sql: %s,%v", err, s, e)
	}

	// select
	selectBuilder := sa.SelectBuilder("*").From(table).
		Where(sq.Eq{"name": userName})
	userRes := []tableUser{}
	err = sa.SelectContext(context.TODO(), selectBuilder, &userRes)
	if err != nil {
		s, _, e := selectBuilder.ToSql()
		t.Fatalf("SelectContext error: %v, sql: %s,%v", err, s, e)
	}
	if !assert.Equal(t, insertNum, len(userRes)) {
		t.Fatalf("select record num: %d, expect: %d", len(userRes), insertNum)
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
	if n != int64(insertNum) {
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
	if n != int64(insertNum) {
		t.Fatalf("RowsAffected n: %d, error: %v", n, err)
	}

	item := tableUser{
		Name: "name",
		UID:  1001,
	}
	updateBuilder = sa.UpdateBuilder(table)
	sqlStr, args, err := sa.SetUpdateColumns(updateBuilder, &item, "id").ToSql()
	if err != nil {
		t.Fatalf("SetUpdateColumns error: %v", err)
	}
	t.Logf("%s,%v", sqlStr, args)
	assert.Equal(t, "UPDATE testuser SET name = ?, uid = ?", sqlStr, "update sql should be equal")
	assert.Equal(t, []interface{}{item.Name, item.UID}, args, "update args should be equal")
}

func TestSqlAgent_ExecSql(t *testing.T) {
	// sql: create database test;
	//      CREATE USER 'test'@'localhost' IDENTIFIED BY 'passwd';
	//      GRANT ALL PRIVILEGES ON test.* TO 'test'@'localhost';
	userTable := "testuser"
	dbName := "myapp_test"
	user := "travis"
	passwd := ""
	testCfg := []dsncfg.Database{
		dsncfg.Database{
			Host:     "127.0.0.1",
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

func TestSqlAgent_ModelColumns(t *testing.T) {
	dbName := "myapp_test"
	user := "travis"
	passwd := ""
	testCfg := dsncfg.Database{
		Host:     "127.0.0.1",
		Port:     3306,
		Name:     dbName,
		Type:     "mysql",
		User:     user,
		Password: passwd,
	}
	sa, err := NewSqlAgent(&testCfg)
	if !assert.Nil(t, err) {
		t.FailNow()
	}
	type testTable struct {
		ID         string
		Name       string
		CreateTime time.Time
	}
	columns := sa.ModelColumns(testTable{}, "id")
	sort.Strings(columns)
	assert.Equal(t, []string{"createtime", "name"}, columns)

	type deriveTable struct {
		testTable
		Col1 int
		Col2 string
	}
	columns = sa.ModelColumns(deriveTable{}, "id")
	sort.Strings(columns)
	assert.Equal(t, []string{"col1", "col2", "createtime", "name",}, columns)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
