package etcdfailover

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"context"
	"sync"
	"fmt"
	"time"
	"errors"
)

// NOTE
// Known Issues:
// 1. 	If etcd cluster becomes unavailable, participants will take 2 * session_timeout_secs to shutdown cleanly


type ElectionState string
const (
	NotParticipant    ElectionState = "NotParticipant"
	Participant       ElectionState = "Participant"
	Leader            ElectionState = "Leader"
)

type ElectionManager struct {
	cfg            Config
	cli            *clientv3.Client
	cliCreated     bool
	session        *concurrency.Session
	sessionCreated bool
	election       *concurrency.Election
	cancel         context.CancelFunc
	state          ElectionState
	evtChan        chan ElectionState

	sessionMonitor chan bool
	mu             sync.Mutex
	wg             sync.WaitGroup
	gophersDone    chan bool
}

type ElectionManagerOption func(*ElectionManager)

func WithEtcdClient(cli *clientv3.Client) ElectionManagerOption {
	return func(em *ElectionManager) {
		em.cli = cli
	}
}

func WithEtcdSession(session *concurrency.Session) ElectionManagerOption {
	return func(em *ElectionManager) {
		em.session = session
	}
}

func NewElectionManager(provided Config, opts ...ElectionManagerOption) *ElectionManager {
	cfg := provided.Validate()
	em := &ElectionManager{
		cfg:    cfg,
		cancel: func () {},
		state:  NotParticipant,
		gophersDone: make(chan bool, 1),
	}
	for _, opt := range opts {
		opt(em)
	}

	//if session is provided, etcd client is not required, hence etcd config not required
	//if client is provided, etcd config is not required, session can be created from provided etcd client
	//when session and client both are not provided, etcd config is required
	if em.session == nil && em.cli == nil {
		cfg.logger.Warn("etcd client not provided, falling back to config.Etcd which will be used to create etcd client and session later")
		em.cfg = em.cfg.ValidateEtcdConfig()
	}

	//to ensure that start proceeds when called for first time
	close(em.gophersDone)
	return em
}

func (em *ElectionManager) Start() (<-chan ElectionState, error) {
	select {
		case <-time.After(5 * time.Second):
			return nil, errors.New("election manager in started/unclean state. stop election manager and retry again")
		case <-em.gophersDone:
			em.cfg.logger.Debug("Attempting to participate")
	}

	em.mu.Lock()
	defer em.mu.Unlock()

	if em.state == NotParticipant {
		em.evtChan = make(chan ElectionState, 1)
		em.gophersDone = make(chan bool, 1)
		var err error
		//ensure consecutive start-ups don't happen
		defer func() {
			em.setState(Participant)
		}()

		if em.session == nil {
			if em.cli == nil {
				em.cliCreated = true
				em.cli, err = clientv3.New(em.cfg.Etcd)
				if err != nil {
					return nil, fmt.Errorf("failed to setup etcd client: %v", err)
				}
				em.cfg.logger.Debug("etcd client created")
			}
			em.sessionCreated = true
			em.session, err = concurrency.NewSession(em.cli, concurrency.WithTTL(em.cfg.SessionTimeoutSecs))
			if err != nil {
				return nil, fmt.Errorf("failed to create session: %v", err)
			}
			em.cfg.logger.Debug("etcd session created")
		}

		em.election = concurrency.NewElection(em.session, em.cfg.ElectionPrefix)
		em.cfg.logger.Debug("etcd election created")

		var cctx context.Context
		cctx, em.cancel = context.WithCancel(context.Background())

		em.sessionMonitor = make(chan bool)
		em.wg.Add(1)
		go func() {
			defer em.wg.Done()
			em.cfg.logger.Debug("observing etcd session expiry")
			select {
			case <-em.sessionMonitor:
				em.cfg.logger.Debug("stopping monitoring of session expiry")
			case <-em.session.Done():
				em.cfg.logger.Error("etcd session expired")
				em.StopAndLog()
			}
		}()

		em.wg.Add(1)
		go func() {
			defer em.wg.Done()
			em.cfg.logger.Debug("starting election campaign")
			if err := em.election.Campaign(cctx, ""); err != nil {
				if err != context.Canceled {
					em.cfg.logger.Errorf("failed during election campaign: %v", err)
					em.StopAndLog()
				}
			} else {
				em.mu.Lock()
				//If Stop and this goroutine were competing for same lock and Stop won, then state can be NotParticipant
				if em.state == Participant {
					em.setState(Leader)
				}
				em.mu.Unlock()
			}
			em.cfg.logger.Debug("stopped election campaign")
		}()

		go func() {
			em.wg.Wait()
			close(em.gophersDone)
		}()

		return em.evtChan, nil
	} else {
		return nil, fmt.Errorf("cannot setup election manager. already started, state = %s", em.state)
	}
}

func (em *ElectionManager) Stop() <-chan error {
	c := make(chan error, 1)

	go func() {
		em.mu.Lock()
		defer em.mu.Unlock()
		if err := em.stop(); err != nil {
			c <- err
		}
		close(c)
	}()

	return c
}

func (em *ElectionManager) StopAndLog() {
	serr := <-em.Stop()
	if serr != nil {
		em.cfg.logger.Warnf("error on stopping election manager: %v", serr)
	}
}

func (em *ElectionManager) Run(runnable func() error) (<-chan error, func() <-chan error, error) {
	em.cfg.logger.Info("starting election participation")
	var (
		electionChan <-chan ElectionState
		err	error
	)
	if electionChan, err = em.Start(); err != nil {
		em.StopAndLog()
		return nil, nil, err
	}

	errChan := make(chan error, 2)
	go func(runnable func() error) {
		em.cfg.logger.Debug("waiting for election event or quit signal")
		for evt := range electionChan {
			switch evt {
			case Participant:
				em.cfg.logger.Info("participant is eligible to become leader")
			case Leader:
				em.cfg.logger.Info("leader among participants")
				if err = runnable(); err != nil {
					em.cfg.logger.Errorf("encountered error in runnable: %v", err)
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
		em.cfg.logger.Infof("stopping election manager in state: %s", em.state)
		//to ensure consecutive cleanups don't happen
		defer func() {
			em.state = NotParticipant
		}()
		close(em.evtChan)

		// check for nil on election, session and cli because its possible that `ElectionManager.Start()` failed somewhere in between
		if em.election != nil {
			em.cfg.logger.Debug("resigning from election")
			close(em.sessionMonitor)
			em.sessionMonitor = nil

			// wait on resign for at least session timeout to ensure that if resign fails in case of n/w partition or etcd disaster, session has reliably expired
			cctx, cc := context.WithTimeout(context.Background(), time.Duration(em.cfg.SessionTimeoutSecs) * time.Second)
			defer cc()
			err := em.election.Resign(cctx)
			if err != nil {
				em.cfg.logger.Warnf("error resigning from election: %v", err)
			}
		}
		em.cancel()

		if em.sessionCreated && em.session != nil {
			em.cfg.logger.Debug("closing etcd session")
			if err := em.session.Close(); err != nil {
				em.cfg.logger.Warnf("error closing session: v%", err.Error())
			}
		}

		if em.cliCreated && em.cli != nil {
			em.cfg.logger.Debug("closing etcd client")
			if err := em.cli.Close(); err != nil {
				return err
			}
		}
	} else {
		em.cfg.logger.Warnf("cannot stop election manager. already stopped, state: %s", em.state)
	}
	return nil
}

func (em *ElectionManager) setState(newState ElectionState) {
	em.cfg.logger.Debugf("new election state received: %s", newState)
	em.state = newState
	em.evtChan <- em.state
}