package etcdfailover

import (
	"time"
	"github.com/coreos/etcd/clientv3"
	"github.com/Flipkart/chihaya/pkg/log"
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
}

func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"etcd":		  		cfg.Etcd,
		"electionPrefix":	cfg.ElectionPrefix,
	}
}

func (cfg Config) Validate() Config {
	validcfg := cfg

	if len(cfg.ElectionPrefix) == 0 {
		validcfg.ElectionPrefix = defaultElectionPrefix
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".ElectionPrefix",
			"provided": cfg.ElectionPrefix,
			"default":  validcfg.ElectionPrefix,
		})
	}

	if cfg.SessionTimeoutSecs <= 0 {
		validcfg.SessionTimeoutSecs = defaultSessionTimeoutSecs
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".SessionTimeoutSecs",
			"provided": cfg.SessionTimeoutSecs,
			"default":  validcfg.SessionTimeoutSecs,
		})
	}

	return validcfg
}

func (cfg Config) ValidateEtcdConfig() Config {
	validcfg := cfg

	if len(cfg.Etcd.Endpoints) == 0 {
		validcfg.Etcd.Endpoints = []string{defaultEtcdEndpoint}
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Etcd",
			"provided": cfg.Etcd,
			"default":  validcfg.Etcd,
		})
	}

	if cfg.Etcd.DialTimeout <= 0 {
		validcfg.Etcd.DialTimeout = defaultDialTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Etcd",
			"provided": cfg.Etcd,
			"default":  validcfg.Etcd,
		})
	}

	return validcfg
}
