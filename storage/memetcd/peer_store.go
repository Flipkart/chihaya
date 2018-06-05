// Package memetcd implements the storage interface for a Chihaya
// BitTorrent tracker keeping peer data in memory, backed by etcd for persistence and auto failover
package memetcd

import (
	"encoding/binary"

	"gopkg.in/yaml.v2"

	"github.com/Flipkart/chihaya/bittorrent"
	"github.com/Flipkart/chihaya/pkg/log"
	"github.com/Flipkart/chihaya/storage"
	"github.com/coreos/etcd/clientv3"
	"strings"
	"github.com/Flipkart/chihaya/storage/memory"
	"context"
	"time"
	"fmt"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/Flipkart/chihaya/pkg/timecache"
	"strconv"
)

// Name is the name by which this peer store is registered with Chihaya.
const Name = "memetcd"

// Default config constants.
const (
	defaultEtcdEndpoint			       = "127.0.0.1:2379"
	defaultEtcdNamespace			   = "tracker/"
	defaultEtcdOpTimeout			   = 3 * time.Second
	defaultBootstrapTimeout		   	   = 60 * time.Second
)

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewPeerStore(icfg interface{}) (storage.PeerStore, error) {
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

	return New(cfg)
}

// Config holds the configuration of a memory PeerStore.
type Config struct {
	EtcdConfig				clientv3.Config	`yaml:"etcd_config"`
	EtcdNamespace			string		  	`yaml:"etcd_namespace"`
	EtcdOpTimeout			time.Duration	`yaml:"etcd_op_timeout"`
	Bootstrap				bool			`yaml:"bootstrap"`
	BootstrapTimeout		time.Duration	`yaml:"bootstrap_timeout"`
	EtcdOpAsync				bool			`yaml:"etcd_op_async"`
	MemoryStore				memory.Config	`yaml:"memory_store"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	fields := log.Fields{
		"name":               Name,
		"etcdConfig":		  cfg.EtcdConfig,
		"etcdNamespace":	  cfg.EtcdNamespace,
		"etcdOpTimeout":	  cfg.EtcdOpTimeout,
		"bootstrap":		  cfg.Bootstrap,
		"bootstrapTimeout":	  cfg.BootstrapTimeout,
		"etcdOpAsync":		  cfg.EtcdOpAsync,
	}

	prefix := "memoryStore."
	for k, v := range cfg.MemoryStore.LogFields() {
		fields[prefix + k] = v
	}

	return fields
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if len(cfg.EtcdConfig.Endpoints) == 0 {
		validcfg.EtcdConfig.Endpoints = []string{defaultEtcdEndpoint}
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".EtcdConfig",
			"provided": cfg.EtcdConfig,
			"default":  validcfg.EtcdConfig,
		})
	}

	if len(cfg.EtcdNamespace) == 0 {
		validcfg.EtcdNamespace = defaultEtcdNamespace
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".EtcdNamespace",
			"provided": cfg.EtcdNamespace,
			"default":  validcfg.EtcdNamespace,
		})
	} else if !strings.HasSuffix(cfg.EtcdNamespace, "/") {
		validcfg.EtcdNamespace += "/"
	}

	if cfg.EtcdOpTimeout <= 0 {
		validcfg.EtcdOpTimeout = defaultEtcdOpTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".EtcdOpTimeout",
			"provided": cfg.EtcdOpTimeout,
			"default":  validcfg.EtcdOpTimeout,
		})
	}

	if cfg.BootstrapTimeout <= 0 {
		validcfg.BootstrapTimeout = defaultBootstrapTimeout
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".BootstrapTimeout",
			"provided": cfg.BootstrapTimeout,
			"default":  validcfg.BootstrapTimeout,
		})
	}

	validcfg.MemoryStore = cfg.MemoryStore.Validate()

	return validcfg
}

// New creates a new PeerStore backed by memory.
func New(provided Config) (storage.PeerStore, error) {
	cfg := provided.Validate()

	etcdClient, err := clientv3.New(cfg.EtcdConfig)
	if err != nil {
		return nil, err
	}
	etcdClient.KV = namespace.NewKV(etcdClient.KV, cfg.EtcdNamespace)
	etcdClient.Watcher = namespace.NewWatcher(etcdClient.Watcher, cfg.EtcdNamespace)
	etcdClient.Lease = namespace.NewLease(etcdClient.Lease, cfg.EtcdNamespace)

	memoryStore, err := memory.New(cfg.MemoryStore)
	if err != nil {
		return nil, err
	}

	ps := &peerStore{
		cfg:         cfg,
		etcd:        etcdClient,
		memoryStore: memoryStore,
		closed:      make(chan struct{}),
	}

	if err = ps.Bootstrap(); err != nil {
		return nil, err
	}

	return ps, nil
}

type serializedPeer []byte

func newPeerKey(p *bittorrent.Peer) serializedPeer {
	b := make([]byte, 20+2+len(p.IP.IP))
	copy(b[:20], p.ID[:])
	binary.BigEndian.PutUint16(b[20:22], p.Port)
	copy(b[22:], p.IP.IP)
	return b
}

const (
	EtcdKeySep 					= "/"
	EtcdAnnouncePrefix 			= "announce"
	EtcdAnnounceSeederPrefix 	= "s" 				//len 1
	EtcdAnnounceLeecherPrefix	= "l"				//len 1
)

func newAnnounceKey(ih *bittorrent.InfoHash, p *bittorrent.Peer, seeder bool) ([]byte) {
	prefix, sep := []byte(EtcdAnnouncePrefix), []byte(EtcdKeySep)
	var peerType []byte
	if seeder {
		peerType = []byte(EtcdAnnounceSeederPrefix)
	} else {
		peerType = []byte(EtcdAnnounceLeecherPrefix)
	}

	var pk serializedPeer
	if p != nil {
		pk = newPeerKey(p)
	}

	key := make([]byte, 0, len(prefix) + len(sep) * 3 + len(ih) + len(peerType) + len(pk))
	key = append(key, prefix...)
	key = append(key, sep...)
	key = append(key, ih[:]...)
	key = append(key, sep...)
	key = append(key, peerType...)
	key = append(key, sep...)
	key = append(key, pk...)

	return key
}

func decodeAnnounceKey(key []byte) (*bittorrent.InfoHash, []byte, bool) {
	prefix, sep := []byte(EtcdAnnouncePrefix), []byte(EtcdKeySep)

	offset := len(prefix) + len(sep)
	ih := bittorrent.InfoHashFromBytes(key[offset:offset+20])

	offset += len(ih) + len(sep)
	peerType := key[offset:offset + 1]
	var seeder bool
	if string(peerType) == EtcdAnnounceSeederPrefix {
		seeder = true
	} else {
		seeder = false
	}

	offset += 1 + len(sep)
	peerKey := key[offset:]

	return &ih, peerKey, seeder
}

type peerStore struct {
	cfg         Config
	etcd        *clientv3.Client
	memoryStore storage.PeerStore
	closed      chan struct{}
}

var _ storage.PeerStore = &peerStore{}

func (ps *peerStore) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if err := ps.memoryStore.PutSeeder(ih, p); err != nil {
		return err
	}

	if ps.cfg.EtcdOpAsync {
		go func() {
			if err := ps.etcdPutOrUpdatePeer(ih, p, true); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := ps.etcdPutOrUpdatePeer(ih, p, true); err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStore) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if err := ps.memoryStore.DeleteSeeder(ih, p); err != nil {
		return err
	}

	if ps.cfg.EtcdOpAsync {
		go func() {
			if err := ps.etcdDeletePeer(ih, p, true); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := ps.etcdDeletePeer(ih, p, true); err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStore) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if err := ps.memoryStore.PutLeecher(ih, p); err != nil {
		return err
	}

	if ps.cfg.EtcdOpAsync {
		go func() {
			if err := ps.etcdPutOrUpdatePeer(ih, p, false); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := ps.etcdPutOrUpdatePeer(ih, p, false); err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStore) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if err := ps.memoryStore.DeleteLeecher(ih, p); err != nil {
		return err
	}

	if ps.cfg.EtcdOpAsync {
		go func() {
			if err := ps.etcdDeletePeer(ih, p, false); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := ps.etcdDeletePeer(ih, p, false); err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStore) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if err := ps.memoryStore.GraduateLeecher(ih, p); err != nil {
		return err
	}

	if ps.cfg.EtcdOpAsync {
		go func() {
			if err := ps.etcdGraduateLeecher(ih, p); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := ps.etcdGraduateLeecher(ih, p); err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStore) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	peers, err = ps.memoryStore.AnnouncePeers(ih, seeder, numWant, announcer)
	return
}

func (ps *peerStore) ScrapeSwarm(ih bittorrent.InfoHash, addressFamily bittorrent.AddressFamily) (resp bittorrent.Scrape) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	resp = ps.memoryStore.ScrapeSwarm(ih, addressFamily)
	return
}

// We do serializable reads of announce keys (compared to linearizable by default) for lower latency
// Obvious con is that stale reads might occur which we are fine with in this scenario
func (ps* peerStore) Bootstrap(...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.BootstrapTimeout)
	defer cancel()

	rangeResp, err := ps.etcd.Get(ctx, EtcdAnnouncePrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return fmt.Errorf("failed to do etcd operation, bootstrap of all infohash-peer announces: %v", err)
	}

	announces := make([]interface{}, 0, rangeResp.Count)
	log.Info(fmt.Sprintf("existing announces found = %d", rangeResp.Count))
	for _, kv := range rangeResp.Kvs {
		ih, peerKey, seeder := decodeAnnounceKey(kv.Key)
		clock, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			log.Error(fmt.Sprintf("Invalid clock value read for key-value %s-%s: %v", kv.Key, string(kv.Value), err))
			continue
		}
		announces = append(announces, memory.NewAnnounce(ih, peerKey, seeder, clock))
	}

	if err = ps.memoryStore.Bootstrap(announces...); err != nil {
		return fmt.Errorf("error bootstrapping memory store with announces from etcd: %v", err)
	}

	return nil
}

func (ps *peerStore) Stop() <-chan error {
	c := make(chan error)
	go func() {
		close(ps.closed)
		if err := <-ps.memoryStore.Stop(); err != nil {
			c <- err
		}

		if err := ps.etcd.Close(); err != nil {
			c <- err
		}
		close(c)
	}()

	return c
}

func (ps *peerStore) LogFields() log.Fields {
	return ps.cfg.LogFields()
}


// NON-ATOMICITY AND CONSEQUENCES
// ------------------------------
// This method is not atomic for a unique (infohash, peer),
// so etcd operations can interleave with another invoke of this method for same arguments
// etcd v3 does not support renew of lease in a transaction which is the primary blocker for above
// Open github issue: https://github.com/coreos/etcd/issues/8550
// Because of non-atomicity, method can result in orphaned leases in etcd which are also bound to expire at some point
// Since tracker will rarely receive concurrent requests for same (infohash, peer), we should do ok with above
// In-memory locks can also be considered in single active tracker setups but not worth the trouble, in my opinion
//
// PERF OPTIMIZATION USING LOCAL TIMESTAMP AS VALUE OF ANNOUNCE KEYS
// -----------------------------------------------------------------
// Another odd thing this method does is to put local unix timestamp as value of announce keys
// which seems counter-intuitive because GC is automatic using leases
// The reason behind this is to make bootstrapping of a new tracker instance faster
// by avoiding etcd call to fetch current lease TTL for every announce key
// If there is significant clock skew across tracker nodes this will result in some keys getting evaluated
// with incorrect TTL because of local time used as frame of reference
//
func (ps *peerStore) etcdPutOrUpdatePeer(ih bittorrent.InfoHash, p bittorrent.Peer, seeder bool) error {
	current := fmt.Sprintf("%d", timecache.NowUnixNano())
	key := string(newAnnounceKey(&ih, &p, seeder))
	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.EtcdOpTimeout)
	defer cancel()

	getResp, err := ps.etcd.Get(ctx, key, clientv3.WithLimit(1), clientv3.WithKeysOnly())
	if err != nil {
		return fmt.Errorf("failed to do etcd operation, get for %s: %v", key, err)
	}
	if getResp.Count == 0 {
		leaseResp, err := ps.etcd.Grant(ctx, int64(ps.cfg.MemoryStore.PeerLifetime.Seconds()))
		if err != nil {
			return fmt.Errorf("failed to do etcd operation, grant lease for %s: %v", key, err)
		}
		if _, err = ps.etcd.Put(ctx, key, current, clientv3.WithLease(leaseResp.ID)); err != nil {
			return fmt.Errorf("failed to do etcd operation, put key-value %s-%s: %v", key, current, err)
		}
	} else {
		if _, err := ps.etcd.KeepAliveOnce(ctx, clientv3.LeaseID(getResp.Kvs[0].Lease)); err != nil {
			return fmt.Errorf("failed to do etcd operation, renew lease for %s: %v", key, err)
		}
		if _, err = ps.etcd.Put(ctx, key, current, clientv3.WithIgnoreLease()); err != nil {
			return fmt.Errorf("failed to do etcd operation, update key-value %s-%s: %v", key, current, err)
		}
	}

	return nil
}

func (ps *peerStore) etcdDeletePeer(ih bittorrent.InfoHash, p bittorrent.Peer, seeder bool) error {
	key := string(newAnnounceKey(&ih, &p, seeder))
	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.EtcdOpTimeout)
	defer cancel()

	if _, err := ps.etcd.Delete(ctx, key); err != nil {
		return fmt.Errorf("failed to do etcd operation, delete for %s: %v", key, err)
	}

	return nil
}

func (ps *peerStore) etcdGraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	current := fmt.Sprintf("%d", timecache.NowUnixNano())
	lkey := string(newAnnounceKey(&ih, &p, false))
	skey := string(newAnnounceKey(&ih, &p, true))
	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.EtcdOpTimeout)
	defer cancel()

	leaseResp, err := ps.etcd.Grant(ctx, int64(ps.cfg.MemoryStore.PeerLifetime.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to do etcd operation, grant lease for %s: %v", skey, err)
	}

	tx := ps.etcd.Txn(ctx)
	_, err = tx.If(
		clientv3.Compare(clientv3.Version(lkey), ">", 0),
	).Then(
		clientv3.OpDelete(lkey),
		clientv3.OpPut(skey, current, clientv3.WithLease(leaseResp.ID)),
	).Else(
		clientv3.OpPut(skey, current, clientv3.WithLease(leaseResp.ID)),
	).Commit()
	if err != nil {
		return fmt.Errorf("failed to do etcd operation, graduate leecher txn for %s to %s: %v", lkey, skey, err)
	}

	return nil
}