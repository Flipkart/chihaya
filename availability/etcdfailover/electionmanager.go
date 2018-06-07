package etcdfailover

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"context"
	"sync"
	"gopkg.in/yaml.v2"
	"github.com/Flipkart/chihaya/pkg/log"
	"fmt"
	"time"
)

type ElectionState string
const (
	NotParticipant    ElectionState = "NotParticipant"
	Participant       ElectionState = "Participant"
	Leader            ElectionState = "Leader"
)

func NewElectionManager(icfg interface{}) (*ElectionManager, error) {
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

type ElectionManager struct {
	cfg      Config

	cli      *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	cancel   context.CancelFunc

	mu		 sync.Mutex
	state    ElectionState
	evtChan  *SafeSendChan
}

func New(provided Config) (*ElectionManager, error) {
	cfg := provided.Validate()

	return &ElectionManager{
		cfg:    cfg,
		cancel: func () {},
		state:  NotParticipant,
	}, nil
}

func (em *ElectionManager) Start() (<-chan ElectionState, error) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.state == NotParticipant {
		em.evtChan = NewSafeSendChan()
		var err error
		//ensure consecutive start-ups don't happen
		defer func() {
			em.setState(Participant)
		}()

		em.cli, err = clientv3.New(em.cfg.Etcd)
		if err != nil {
			return nil, fmt.Errorf("failed to setup etcd client: %v", err)
		}
		log.Debug("etcd client created")

		em.session, err = concurrency.NewSession(em.cli, concurrency.WithTTL(em.cfg.SessionTimeoutSecs))
		if err != nil {
			return nil, fmt.Errorf("failed to create session: %v", err)
		}
		log.Debug("etcd session created")

		em.election = concurrency.NewElection(em.session, em.cfg.ElectionPrefix)
		log.Debug("etcd election created")

		var cctx context.Context
		cctx, em.cancel = context.WithCancel(context.Background())

		go func() {
			log.Debug("observing etcd session expiry")
			<-em.session.Done()
			log.Error("etcd session expired")
			em.StopAndLog()
		}()

		go func() {
			log.Debug("starting election campaign")
			if err := em.election.Campaign(cctx, ""); err != nil {
				log.Error(fmt.Errorf("failed during election campaign: %v", err))
				em.StopAndLog()
			} else {
				em.setState(Leader)
			}
			log.Debug("stopped election campaign")
		}()

		return em.evtChan.RecvChan(), nil
	} else {
		return nil, fmt.Errorf("cannot setup election manager. already started, state = %s", em.state)
	}
}

func (em *ElectionManager) Stop() <-chan error {
	c := make(chan error)

	go func() {
		em.mu.Lock()
		defer em.mu.Unlock()
		if err := em.stop(); err != nil {
			c <- err
			return
		}
		close(c)
	}()

	return c
}

func (em *ElectionManager) StopAndLog() {
	serr := <-em.Stop()
	if serr != nil {
		log.Warn(fmt.Errorf("error on stopping election manager: %v", serr))
	}
}

func (em *ElectionManager) Run(runnable func() error) (<-chan error, func() <-chan error, error) {
	log.Info("starting election participation")
	var (
		electionChan <-chan ElectionState
		err	error
	)
	if electionChan, err = em.Start(); err != nil {
		em.StopAndLog()
		return nil, nil, err
	}

	errChan := make(chan error)
	go func(runnable func() error) {
		log.Debug("waiting for election event or quit signal")
		for evt := range electionChan {
			switch evt {
			case Participant:
				log.Info("tracker is eligible to become leader")
			case Leader:
				log.Info("leader among trackers")
				if err = runnable(); err != nil {
					errChan <- err
				}
			}
		}
		errChan <- fmt.Errorf("failed in election")
	}(runnable)

	return errChan, em.Stop, nil
}

//method not thread-safe, ensure it is called after acquiring mutex of election manager
func (em *ElectionManager) stop() error {
	if em.state != NotParticipant {
		log.Info("stopping election manager in state: " + em.state)
		//to ensure consecutive cleanups don't happen
		defer func() {
			em.state = NotParticipant
		}()
		em.evtChan.Close()

		// check for nil on election, session and cli because its possible that `ElectionManager.Start()` failed somewhere in between
		if em.election != nil {
			log.Debug("resigning from election")
			// wait on resign for at least session timeout to ensure that if resign fails in case of n/w partition or etcd disaster, session has reliably expired
			cctx, _ := context.WithTimeout(context.Background(), time.Duration(em.cfg.SessionTimeoutSecs) * time.Second)
			err := em.election.Resign(cctx)
			if err != nil {
				log.Warn("error resigning from election: " + err.Error())
			}
		}
		em.cancel()

		if em.session != nil {
			log.Debug("closing etcd session")
			if err := em.session.Close(); err != nil {
				log.Warn("error closing session: " + err.Error())
			}
		}

		if em.cli != nil {
			log.Debug("closing etcd client")
			if err := em.cli.Close(); err != nil {
				return err
			}
		}
	} else {
		log.Error("cannot stop election manager. already stopped, state: " + em.state)
	}
	return nil
}

func (em *ElectionManager) setState(newState ElectionState) {
	log.Debug("new election state received: " + newState)
	em.state = newState
	if err := em.evtChan.Send(em.state); err != nil {
		log.Warn(err)
	}
}

func (em *ElectionManager) LogFields() log.Fields {
	return em.cfg.LogFields()
}