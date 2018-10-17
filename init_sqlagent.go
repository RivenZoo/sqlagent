package sqlagent

import (
	"github.com/RivenZoo/dsncfg"
	"sync"
	"gopkg.in/yaml.v2"
	"path"
	"strings"
	"io/ioutil"
	"encoding/json"
)

var (
	initOnce = &sync.Once{}
)

// Init module SqlAgent with database config.
func Init(cfg *dsncfg.Database) error {
	return initSqlAgent(cfg)
}

// Init module SqlAgent with database config.
// cfgFile: config file path, support file type [.json | .yaml/.yml], default decoder is json.
func InitFromConfig(cfgFile string) error {
	cfg, err := readDBConfig(cfgFile)
	if err != nil {
		return err
	}
	return initSqlAgent(cfg)
}

// cfgFile: config file path, support file type [.json | .yaml/.yml], default decoder is json.
func readDBConfig(cfgFile string) (*dsncfg.Database, error) {
	c, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return nil, err
	}

	ext := path.Ext(cfgFile)
	ext = strings.ToLower(ext)

	dbCfg := &dsncfg.Database{}
	switch ext {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(c, dbCfg)
	default:
		// default: .json
		err = json.Unmarshal(c, dbCfg)
	}
	if err != nil {
		return nil, err
	}
	return dbCfg, nil
}

func setDefaultDBParameters(cfg *dsncfg.Database) {
	if cfg.Type == dsncfg.MySql {
		defaultParams := map[string]string{
			"parseTime":  "true",
			"charset":    "utf8mb4,utf8",
			"autocommit": "true",
			"loc":        "Asia%2FShanghai",
		}
		for k, v := range defaultParams {
			if _, ok := cfg.Parameters[k]; !ok {
				cfg.Parameters[k] = v
			}
		}
	}
}

// initSqlAgent init module SqlAgent only once.
func initSqlAgent(cfg *dsncfg.Database) (err error) {
	initOnce.Do(func() {
		setDefaultDBParameters(cfg)
		defaultAgent, err = NewSqlAgent(cfg)
	})
	return
}
