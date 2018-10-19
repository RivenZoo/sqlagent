package sqlagent

import (
	"testing"
	"os"
	"path/filepath"
	"io/ioutil"
)

const (
	jsonConfigStr = `{
	"host":     "localhost",
	"port":     3306,
	"name":     "dbName",
	"type":     "mysql",
	"user":     "user",
	"password": "passwd"
}`
)

func createTestConfig(dir, fname string, t *testing.T) string {
	_, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("Stat error: %v", err)
		}
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			t.Fatalf("MkdirAll error: %v", err)
		}
	}
	fpath := filepath.Join(dir, fname+".json")
	if _, err = os.Stat(fpath); err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("Stat error: %v", err)
		}
		err = ioutil.WriteFile(fpath, []byte(jsonConfigStr), os.ModePerm)
		if err != nil {
			t.Fatalf("Create file error: %v", err)
		}
	}
	return fpath
}

func rmTestconfig(fpath string) {
	os.RemoveAll(fpath)
}

func TestReadConfig(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}
	fpath := createTestConfig(pwd, defaultDBConfigFileName, t)
	defer rmTestconfig(fpath)

	cfg, err := readDBConfig(fpath)
	if err != nil {
		t.Fatalf("readDBConfig error: %v", err)
	}
	t.Logf("db config: %v", cfg)
	if cfg.Host == "" || cfg.Type == "" {
		t.Fatalf("decode config file fail")
	}
}

func TestFindInDir(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}

	found, err := findInDir(pwd, defaultDBConfigFileName)
	if err != errorNotFoundDBConfig {
		t.Fatalf("findInDir return error: %v", err)
	}
	fpath := createTestConfig(pwd, defaultDBConfigFileName, t)
	defer rmTestconfig(fpath)

	found, err = findInDir(pwd, defaultDBConfigFileName)
	if err != nil {
		t.Fatalf("findInDir error: %v", err)
	}
	t.Logf("found %s", found)
}

func TestFindDBConfig(t *testing.T) {
	if fpath := findDBConfig(1, "config"); fpath != "" {
		t.Fatalf("findDBConfig found %s", fpath)
	}

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}

	label := "prod"
	os.Setenv(envDBLabel, label)
	fpath := createTestConfig(filepath.Join(pwd, "config"), defaultDBConfigFileName+"-"+label, t)
	defer rmTestconfig(fpath)

	if fpath := findDBConfig(1, "config"); fpath == "" {
		t.Fatalf("findDBConfig file not found")
	}
}

func TestDetectDBConfig(t *testing.T) {
	if fpath := detectDBConfig(); fpath != "" {
		t.Fatalf("detectDBConfig found %s", fpath)
	}

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}
	label := "prod"
	os.Setenv(envDBLabel, label)
	fpath := createTestConfig(filepath.Join(pwd, "config"), defaultDBConfigFileName+"-"+label, t)
	defer rmTestconfig(fpath)

	envConfigPath := fpath
	os.Setenv(envDBConfig, envConfigPath)
	if fpath := detectDBConfig(); fpath != envConfigPath {
		t.Fatalf("detectDBConfig found %s", fpath)
	}
}
