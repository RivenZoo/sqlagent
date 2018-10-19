# Sqlagent
A golang sql wrap with sqlx and squirrel

Create instance with db config

```go
cfg := dsncfg.Database{
    Host:     "localhost",
    Port:     3306,
    Name:     "test",
    Type:     "mysql",
    User:     "admin",
    Password: "passwd",
}
sa, err := NewSqlAgent(cfg)
```

Init with config file

```
$ ls
database.json
$ cat database.json
{
	"host":     "localhost",
	"port":     3306,
	"name":     "dbName",
	"type":     "mysql",
	"user":     "user",
	"password": "passwd"
}
```

```go
InitFromConfig("database.json")
```

Init with env variable

- DB_CONFIG set config file path
- DB_LABEL set config file name pattern: database-$DB_LABEL.[json|yaml|yml]

```
$ echo $DB_CONFIG
/etc/database.yaml
$ ls /etc/database.yaml
/etc/database.yaml
```

```
$ pwd
/data/apps
$ echo $DB_LABEL
prod
$ ls ./config/database*
./config/database-prod.json
```

```go
InitFromEnv()
```

Insert

```go
insertBuilder := InsertBuilder(table).
		Columns("name", "uid").
		Values(userName, uid)
_, err := ExecContext(context.TODO(), insertBuilder)
```

Delete

```go
delBuilder := DeleteBuilder(table).
    Where("name=?", userName)
res, err = ExecContext(context.TODO(), delBuilder)
```

Update

```go
updateBuilder := UpdateBuilder(table).Where(sq.Eq{"name": oldUserName}).
        Set("name", userName)
res, err := ExecContext(context.TODO(), updateBuilder)
```

Select

```go
selectBuilder := SelectBuilder("*").From(table).
    Where(sq.Eq{"name": userName})
userRes := []*tableUser{}
err = SelectContext(context.TODO(), selectBuilder, &userRes)
```

Use raw sqlx.DB

```go
DB().SetMaxIdleConns(2)
DB().SetMaxOpenConns(4)
```

## License

Sqlagent is released under the
[MIT License](http://www.opensource.org/licenses/MIT).