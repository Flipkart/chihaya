// Package memetcd implements the storage interface for a Chihaya
// BitTorrent tracker keeping peer data in memory, backed by etcd for persistence and auto failover
package memetcd

import (
	"encoding/binary"

	"gopkg.in/yaml.v2"

	"bytes"
	"context"
	"fmt"
	"github.com/Flipkart/chihaya/bittorrent"
	"github.com/Flipkart/chihaya/pkg/log"
	"github.com/Flipkart/chihaya/pkg/timecache"
	"github.com/Flipkart/chihaya/storage"
	"github.com/Flipkart/chihaya/storage/memory"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"strconv"
	"strings"
	"time"
)

// Name is the name by which this peer store is registered with Chihaya.
const Name = "memetcd"

// Default config constants.
const (
	defaultEtcdEndpoint     = "127.0.0.1:2379"
	defaultEtcdNamespace    = "tracker/"
	defaultEtcdOpTimeout    = 3 * time.Second
	defaultBootstrapTimeout = 60 * time.Second
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
	EtcdConfig       clientv3.Config `yaml:"etcd_config"`
	EtcdNamespace    string          `yaml:"etcd_namespace"`
	EtcdOpTimeout    time.Duration   `yaml:"etcd_op_timeout"`
	BootstrapTimeout time.Duration   `yaml:"bootstrap_timeout"`
	EtcdOpAsync      bool            `yaml:"etcd_op_async"`
	MemoryStore      memory.Config   `yaml:"memory_store"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	fields := log.Fields{
		"name":             Name,
		"etcdConfig":       cfg.EtcdConfig,
		"etcdNamespace":    cfg.EtcdNamespace,
		"etcdOpTimeout":    cfg.EtcdOpTimeout,
		"bootstrapTimeout": cfg.BootstrapTimeout,
		"etcdOpAsync":      cfg.EtcdOpAsync,
	}

	prefix := "memoryStore."
	for k, v := range cfg.MemoryStore.LogFields() {
		fields[prefix+k] = v
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

// New creates a new PeerStore backed by etcd.
func New(provided Config) (storage.PeerStore, error) {
	cfg := provided.Validate()

	etcdClient, err := clientv3.New(cfg.EtcdConfig)
	if err != nil {
		return nil, err
	}
	etcdClient.KV = namespace.NewKV(etcdClient.KV, cfg.EtcdNamespace)
	etcdClient.Watcher = namespace.NewWatcher(etcdClient.Watcher, cfg.EtcdNamespace)
	etcdClient.Lease = namespace.NewLease(etcdClient.Lease, cfg.EtcdNamespace)

	announces, err := loadAnnounces(etcdClient, cfg.BootstrapTimeout)
	if err != nil {
		return nil, err
	}
	memoryStore, err := memory.NewWithAnnounces(cfg.MemoryStore, announces)
	if err != nil {
		return nil, err
	}

	ps := &peerStore{
		cfg:         cfg,
		etcd:        etcdClient,
		memoryStore: memoryStore,
		closed:      make(chan struct{}),
	}
	return ps, nil
}

// We do serializable reads of announce keys (compared to linearizable by default) for lower latency
// Obvious con is that stale reads might occur which we are fine with in this scenario
func loadAnnounces(etcd *clientv3.Client, timeout time.Duration) ([]memory.Announce, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rangeResp, err := etcd.Get(ctx, EtcdAnnouncePrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, fmt.Errorf("failed to do etcd operation, bootstrap of all infohash-peer announces: %v", err)
	}

	announces := make([]memory.Announce, 0, rangeResp.Count)
	log.Info(fmt.Sprintf("existing announces found = %d", rangeResp.Count))
	for _, kv := range rangeResp.Kvs {
		ih, peerKey, seeder, err := decodeAnnounceKey(kv.Key)
		if err != nil {
			log.Error(fmt.Sprintf("error decoding announce key %s: %v", kv.Key, err))
			continue
		}
		clock, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			log.Error(fmt.Sprintf("Invalid clock value read for key-value %s-%s: %v", kv.Key, kv.Value, err))
			continue
		}
		announces = append(announces, memory.NewAnnounce(ih, peerKey, seeder, clock))
	}

	return announces, nil
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
	EtcdKeySep                = "/"
	EtcdAnnouncePrefix        = "announce"
	EtcdAnnounceSeederPrefix  = "s" //len 1
	EtcdAnnounceLeecherPrefix = "l" //len 1
)

// Format of announce key in etcd:
// <announce_prefix><sep><infohash><sep><peer_type><sep><peer_key>
// peer key as returned by newPeerKey and uniquely identifies a peer in swarm
// peer type can either be s(seeder) or l(leecher)
func newAnnounceKey(ih *bittorrent.InfoHash, p *bittorrent.Peer, seeder bool) []byte {
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

	key := bytes.Join([][]byte{
		[]byte(EtcdAnnouncePrefix),
		ih[:],
		peerType,
		pk,
	}, []byte(EtcdKeySep))

	return key
}

func decodeAnnounceKey(key []byte) (*bittorrent.InfoHash, []byte, bool, error) {
	announceKeyReader := func() func() ([]byte, error) {
		sep := []byte(EtcdKeySep)
		partLengths := []int{len([]byte(EtcdAnnouncePrefix)), 20, 1}
		begin, end, currPart := 0, 0, 0
		return func() ([]byte, error) {
			if currPart == len(partLengths)+1 {
				//all parts of announce key have been read
				return nil, nil
			}
			if currPart < len(partLengths) {
				end = begin + partLengths[currPart]
				if end > len(key) {
					return nil, fmt.Errorf("invalid key, length:%d shorter than expected:%d", len(key), end)
				}
				read := key[begin:end]
				begin = end + len(sep)
				currPart += 1
				return read, nil
			} else {
				//this is the last part containing peer key, read till the end
				read := key[begin:]
				currPart += 1
				return read, nil
			}
		}
	}()

	totalParts := 4
	parts := make([][]byte, 0, totalParts)
	for {
		part, err := announceKeyReader()
		if err == nil {
			if len(part) == 0 {
				//empty part received, decoding finished
				if len(parts) == totalParts {
					//all parts received, exit for loop
					break
				}
				err = fmt.Errorf("got=%d less than expected=%d parts of announce key", len(parts), totalParts)
			} else {
				if len(parts) == totalParts {
					err = fmt.Errorf("got more than expected=%d parts of announce key", totalParts)
				}
			}
		}
		if err != nil {
			return nil, nil, false, err
		}
		parts = append(parts, part)
	}

	prefix := string(parts[0])
	if EtcdAnnouncePrefix != prefix {
		return nil, nil, false, fmt.Errorf("expected prefix=%s, got=%s", EtcdAnnouncePrefix, prefix)
	}
	ih := bittorrent.InfoHashFromBytes(parts[1])
	seeder := string(parts[2]) == EtcdAnnounceSeederPrefix
	return &ih, parts[3], seeder, nil
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

	memOp := func() error {
		return ps.memoryStore.PutSeeder(ih, p)
	}
	etcdOp := func() error {
		return ps.etcdPutOrUpdatePeer(ih, p, true)
	}
	return ps.doOp(memOp, etcdOp)
}

func (ps *peerStore) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	memOp := func() error {
		return ps.memoryStore.DeleteSeeder(ih, p)
	}
	etcdOp := func() error {
		err := ps.etcdDeletePeer(ih, p, true)
		if err == storage.ErrResourceDoesNotExist {
			log.Error(fmt.Sprintf("deleteseeder: announce for infohash=%v, peer=%v does not exist in etcd", ih, p.ID))
			return nil
		}
		return err
	}
	return ps.doOp(memOp, etcdOp)
}

func (ps *peerStore) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	memOp := func() error {
		return ps.memoryStore.PutLeecher(ih, p)
	}
	etcdOp := func() error {
		return ps.etcdPutOrUpdatePeer(ih, p, false)
	}
	return ps.doOp(memOp, etcdOp)
}

func (ps *peerStore) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	memOp := func() error {
		return ps.memoryStore.DeleteLeecher(ih, p)
	}
	etcdOp := func() error {
		err := ps.etcdDeletePeer(ih, p, false)
		if err == storage.ErrResourceDoesNotExist {
			log.Error(fmt.Sprintf("deleteleecher: announce for infohash=%v, peer=%v does not exist in etcd", ih, p.ID))
			return nil
		}
		return err
	}
	return ps.doOp(memOp, etcdOp)
}

func (ps *peerStore) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	memOp := func() error {
		return ps.memoryStore.GraduateLeecher(ih, p)
	}
	etcdOp := func() error {
		return ps.etcdGraduateLeecher(ih, p)
	}
	return ps.doOp(memOp, etcdOp)
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

func (ps *peerStore) Stop() <-chan error {
	c := make(chan error, 2)
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

// RACES IN ETCD OPERATIONS FOR (INFOHASH, PEERKEY) WHEN ETCD ASYNC OP IS TRUE
// ---------------------------------------------------------------------------
// There can be a race among deleteseeder, graduateleecher or any other etcdop for same ih,p when ps.cfg.EtcdOpAsync == true
// This should be a rare event unless torrent clients are misbehaving
// or etcd goes down and subsequently previous etcd operations block long enough
// In event of a race, etcd operations will not be consistent but responses to client will be since they are served from in-memory store
// At worst in case of process restart, some announces will be extra or missing which should be ok really
// Tradeoff is less code complexity presently for possibility of races
//
// SUGGESTED FIX:
// If above races do end up to be a concern, we need a queue of etcd ops and multiple consumers draining them
// such that (ih,p) operations are ordered wrt each other.
//
func (ps *peerStore) doOp(memOp func() error, etcdOp func() error) error {
	if ps.cfg.EtcdOpAsync {
		if err := memOp(); err != nil {
			return err
		}
		go func() {
			if err := etcdOp(); err != nil {
				log.Error(err)
			}
		}()
	} else {
		if err := etcdOp(); err != nil {
			return err
		}
		if err := memOp(); err != nil {
			return err
		}
	}
	return nil
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

	resp, err := ps.etcd.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to do etcd operation, delete for %s: %v", key, err)
	}
	if resp.Deleted == 0 {
		return storage.ErrResourceDoesNotExist
	}
	if resp.Deleted > 0 {
		log.Warn(fmt.Sprintf("Keys deleted=%d for etcd key prefix=%s, expected count=1", resp.Deleted, key))
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
