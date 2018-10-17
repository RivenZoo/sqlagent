package sqlagent

import (
	"github.com/RivenZoo/dsncfg"
	"sync"
	"gopkg.in/yaml.v2"
	"path"
	"strings"
	"io/ioutil"
	"encoding/json"
	"os"
	"fmt"
	"path/filepath"
)

const (
	envDBConfig = "DB_CONFIG"
	envDBLabel  = "DB_LABEL"

	defaultDBConfigFileName = "database"
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

// InitFromEnv use Env variable to detect config file and init SqlAgent with first found config file.
// Use env "DB_CONFIG" to set config file path.
// If config file env not set, will find specific file name in a list of dirs.
// File name format:
//   If env "DB_LABEL" set, format is "database-$DB_LABEL.[json | yaml/yml]"
//   Default file name is "database.[json | yaml/yml]"
// Search dirs in order:
//   ./ ./config ./../ ./../config ./../../ ./../../config
func InitFromEnv() error {
	cfgFile := detectDBConfig()
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		return errorNotFoundDBConfig
	}
	return InitFromConfig(cfgFile)
}

func findInDir(dir, filePrefix string) (string, error) {
	suffix := []string{".json", ".yaml", ".yml"}
	for i := range suffix {
		fn := filepath.Join(dir, filePrefix) + suffix[i]
		match, err := filepath.Glob(fn)

		if err == nil && len(match) > 0 {
			return match[0], nil
		}
	}
	return "", errorNotFoundDBConfig
}

func findDBConfig(lvl int, subdir ...string) (cfgFile string) {
	cfgFname := defaultDBConfigFileName
	label := os.Getenv(envDBLabel)
	if label != "" {
		cfgFname = fmt.Sprintf("%s-%s", cfgFname, label)
	}
	curdir, err := os.Getwd()
	if err != nil {
		return
	}
	searchDir := curdir
	for i := 0; i < lvl; i++ {
		cfgFile, err = findInDir(searchDir, cfgFname)
		if err == nil {
			return
		}
		for i := range subdir {
			cfgFile, err = findInDir(path.Join(searchDir, subdir[i]), cfgFname)
			if err == nil {
				return
			}
		}
		upDir := filepath.Dir(searchDir)
		if upDir == searchDir {
			break
		}
		searchDir = upDir
	}
	return
}

func detectDBConfig() string {
	cfgFile := os.Getenv(envDBConfig)
	if _, err := os.Stat(cfgFile); !os.IsNotExist(err) {
		// config file exist
		return cfgFile
	}

	return findDBConfig(3, "config")
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
