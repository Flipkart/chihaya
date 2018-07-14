package etcdfailover

import (
	"time"
	"github.com/coreos/etcd/clientv3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
	"fmt"
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
	logger             *zap.SugaredLogger
}

func NewConfig(icfg interface{}, logger *zap.SugaredLogger) (cfg Config, err error) {
	// Marshal the config back into bytes.
	bytes, err := yaml.Marshal(icfg)
	if err != nil {
		return
	}
	// Unmarshal the bytes into the proper config type.
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return
	}
	cfg.logger = logger
	return
}

func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.logger == nil {
		validcfg.logger = getDefaultLogger()
		validcfg.logger.Info("setup zap logger with default production values, will log to stdout at info level")
	}

	if len(cfg.ElectionPrefix) == 0 {
		validcfg.ElectionPrefix = defaultElectionPrefix
		validcfg.logger.Warnw("falling back to default configuration",
			"name", Name + ".ElectionPrefix",
			"provided", cfg.ElectionPrefix,
			"default",  validcfg.ElectionPrefix,
		)
	}

	if cfg.SessionTimeoutSecs <= 0 {
		validcfg.SessionTimeoutSecs = defaultSessionTimeoutSecs
		validcfg.logger.Warnw("falling back to default configuration",
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
		validcfg.logger.Warnw("falling back to default configuration",
			"name",     Name + ".Etcd",
			"provided", cfg.Etcd.Endpoints,
			"default",  validcfg.Etcd.Endpoints,
		)
	}

	if cfg.Etcd.DialTimeout <= 0 {
		validcfg.Etcd.DialTimeout = defaultDialTimeout
		validcfg.logger.Warnw("falling back to default configuration",
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
		panic(fmt.Sprintf("err creating zap logger: %v", err))
	}
	return zapLogger.Sugar()
}
