package memetcd

import (
	"testing"

	"time"

	s "github.com/Flipkart/chihaya/storage"
	"github.com/Flipkart/chihaya/storage/memory"
	"github.com/coreos/etcd/clientv3"
)

func createNew() s.PeerStore {
	ps, err := New(Config{
		EtcdConfig: clientv3.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		EtcdNamespace: "tracker/",
		EtcdOpAsync: false,
		EtcdOpTimeout: 3 * time.Second,
		BootstrapTimeout: 10 * time.Second,
		MemoryStore: memory.Config{
			ShardCount: 1024,
			GarbageCollectionInterval: 10 * time.Minute,
			PrometheusReportingInterval: 10 * time.Minute,
			PeerLifetime: 30 * time.Minute,
		},
	})

	if err != nil {
		panic(err)
	}
	return ps
}

func TestPeerStore(t *testing.T) {
	s.TestPeerStore(t, createNew())
}

func TestPeerStoreDurability(t *testing.T) {
	peerLifetime := 10 * time.Second
	peerStoreFactory := func() (s.PeerStore) {
		p, err := New(Config{
			EtcdConfig: clientv3.Config{
				Endpoints: []string{"127.0.0.1:2379"},
			},
			EtcdNamespace: "durabletracker/",
			EtcdOpAsync:   false,
			EtcdOpTimeout: 3 * time.Second,
			BootstrapTimeout: 10 * time.Second,
			MemoryStore: memory.Config{
				ShardCount:                  1024,
				GarbageCollectionInterval:   5 * time.Second,
				PrometheusReportingInterval: 5 * time.Second,
				PeerLifetime:                peerLifetime,
			},
		})

		if err != nil {
			panic(err)
		}
		return p
	}
	s.TestPeerStoreDurability(t, peerLifetime, peerStoreFactory)
}