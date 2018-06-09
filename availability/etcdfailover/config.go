package etcdfailover

import (
	"time"
	"github.com/coreos/etcd/clientv3"
	"go.uber.org/zap"
	"log"
	"gopkg.in/yaml.v2"
)

const Name = "etcdfailover"

const (
	defaultEtcdEndpoint = "127.0.0.1:2379"
	defaultDialTimeout = 5 * time.Second
	defaultElectionPrefix = "/tracker-election"
	defaultSessionTimeoutSecs = 30
)

type Config struct {
	Etcd               clientv3.Config `yaml:"etcd"`
	ElectionPrefix     string          `yaml:"election_prefix"`
	SessionTimeoutSecs int             `yaml:"session_timeout_secs"`
	Logger		   	   *zap.SugaredLogger
}

func NewConfig(icfg interface{}, logger *zap.SugaredLogger) (*Config, error) {
	// Marshal the config back into bytes.
	bytes, err := yaml.Marshal(icfg)
	if err != nil {
		return nil, err
	}
	// Unmarshal the bytes into the proper config type.
	var cfg Config
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return nil, err
	}
	cfg.Logger = logger
	return &cfg, nil
}

func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.Logger == nil {
		validcfg.Logger = getDefaultLogger()
		validcfg.Logger.Info("setup zap logger with default production values, will log to stdout at info level")
	}

	if len(cfg.ElectionPrefix) == 0 {
		validcfg.ElectionPrefix = defaultElectionPrefix
		validcfg.Logger.Warnw("falling back to default configuration",
			"name", Name + ".ElectionPrefix",
			"provided", cfg.ElectionPrefix,
			"default",  validcfg.ElectionPrefix,
		)
	}

	if cfg.SessionTimeoutSecs <= 0 {
		validcfg.SessionTimeoutSecs = defaultSessionTimeoutSecs
		validcfg.Logger.Warnw("falling back to default configuration",
			"name",     Name + ".SessionTimeoutSecs",
			"provided", cfg.SessionTimeoutSecs,
			"default",  validcfg.SessionTimeoutSecs,
		)
	}

	return validcfg
}

func (cfg Config) ValidateEtcdConfig() Config {
	validcfg := cfg

	if len(cfg.Etcd.Endpoints) == 0 {
		validcfg.Etcd.Endpoints = []string{defaultEtcdEndpoint}
		validcfg.Logger.Warnw("falling back to default configuration",
			"name",     Name + ".Etcd",
			"provided", cfg.Etcd.Endpoints,
			"default",  validcfg.Etcd.Endpoints,
		)
	}

	if cfg.Etcd.DialTimeout <= 0 {
		validcfg.Etcd.DialTimeout = defaultDialTimeout
		validcfg.Logger.Warnw("falling back to default configuration",
			"name",     Name + ".Etcd",
			"provided", cfg.Etcd.DialTimeout,
			"default",  validcfg.Etcd.DialTimeout,
		)
	}

	return validcfg
}

func getDefaultLogger() *zap.SugaredLogger {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("err creating zap logger: %v", err)
	}
	return zapLogger.Sugar()
}
