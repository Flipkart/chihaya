package etcdfailover

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"context"
	"sync"
	"fmt"
	"time"
)

// NOTE
// Known Issues:
// 1. 	If etcd cluster becomes unavailable, tracker nodes will take 2 * session_timeout_secs to shutdown cleanly


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
	mu             sync.Mutex
	state          ElectionState
	evtChan        *SafeSendChan
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
	}
	for _, opt := range opts {
		opt(em)
	}

	//if session is provided, etcd client is not required, hence etcd config not required
	//if client is provided, etcd config is not required, session can be created from provided etcd client
	//when session and client both are not provided, etcd config is required
	if em.session == nil && em.cli == nil {
		cfg.Logger.Warn("etcd client not provided, falling back to config.Etcd which will be used to create etcd client and session later")
		em.cfg = em.cfg.ValidateEtcdConfig()
	}

	return em
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

		if em.session == nil {
			if em.cli == nil {
				em.cliCreated = true
				em.cli, err = clientv3.New(em.cfg.Etcd)
				if err != nil {
					return nil, fmt.Errorf("failed to setup etcd client: %v", err)
				}
				em.cfg.Logger.Debug("etcd client created")
			}
			em.sessionCreated = true
			em.session, err = concurrency.NewSession(em.cli, concurrency.WithTTL(em.cfg.SessionTimeoutSecs))
			if err != nil {
				return nil, fmt.Errorf("failed to create session: %v", err)
			}
			em.cfg.Logger.Debug("etcd session created")
		}

		em.election = concurrency.NewElection(em.session, em.cfg.ElectionPrefix)
		em.cfg.Logger.Debug("etcd election created")

		var cctx context.Context
		cctx, em.cancel = context.WithCancel(context.Background())

		go func() {
			em.cfg.Logger.Debug("observing etcd session expiry")
			<-em.session.Done()
			em.cfg.Logger.Error("etcd session expired")
			em.StopAndLog()
		}()

		go func() {
			em.cfg.Logger.Debug("starting election campaign")
			if err := em.election.Campaign(cctx, ""); err != nil {
				em.cfg.Logger.Errorf("failed during election campaign: %v", err)
				em.StopAndLog()
			} else {
				em.setState(Leader)
			}
			em.cfg.Logger.Debug("stopped election campaign")
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
		em.cfg.Logger.Warnf("error on stopping election manager: %v", serr)
	}
}

func (em *ElectionManager) Run(runnable func() error) (<-chan error, func() <-chan error, error) {
	em.cfg.Logger.Info("starting election participation")
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
		em.cfg.Logger.Debug("waiting for election event or quit signal")
		for evt := range electionChan {
			switch evt {
			case Participant:
				em.cfg.Logger.Info("tracker is eligible to become leader")
			case Leader:
				em.cfg.Logger.Info("leader among trackers")
				if err = runnable(); err != nil {
					em.cfg.Logger.Errorf("encountered error in runnable: %v", err)
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
		em.cfg.Logger.Infof("stopping election manager in state: %s", em.state)
		//to ensure consecutive cleanups don't happen
		defer func() {
			em.state = NotParticipant
		}()
		em.evtChan.Close()

		// check for nil on election, session and cli because its possible that `ElectionManager.Start()` failed somewhere in between
		if em.election != nil {
			em.cfg.Logger.Debug("resigning from election")
			// wait on resign for at least session timeout to ensure that if resign fails in case of n/w partition or etcd disaster, session has reliably expired
			cctx, _ := context.WithTimeout(context.Background(), time.Duration(em.cfg.SessionTimeoutSecs) * time.Second)
			err := em.election.Resign(cctx)
			if err != nil {
				em.cfg.Logger.Warnf("error resigning from election: %v", err)
			}
		}
		em.cancel()

		if em.sessionCreated && em.session != nil {
			em.cfg.Logger.Debug("closing etcd session")
			if err := em.session.Close(); err != nil {
				em.cfg.Logger.Warnf("error closing session: v%", err.Error())
			}
		}

		if em.cliCreated && em.cli != nil {
			em.cfg.Logger.Debug("closing etcd client")
			if err := em.cli.Close(); err != nil {
				return err
			}
		}
	} else {
		em.cfg.Logger.Errorf("cannot stop election manager. already stopped, state: %s", em.state)
	}
	return nil
}

func (em *ElectionManager) setState(newState ElectionState) {
	em.cfg.Logger.Debugf("new election state received: %s", newState)
	em.state = newState
	if err := em.evtChan.Send(em.state); err != nil {
		em.cfg.Logger.Warn(err)
	}
}