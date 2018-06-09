package etcdfailover

import (
	"testing"
	"fmt"
	"go.uber.org/zap"
	"log"
	"time"
)

type stopper func() <-chan error

var configs []Config
func init() {
	configs = make([]Config, 0, 2)
	configs = append(configs, Config{
		ElectionPrefix: "/test1-election",
		SessionTimeoutSecs: 5,
		Logger: getLogger("em1"),
	})
	configs = append(configs, Config{
		ElectionPrefix: "/test1-election",
		SessionTimeoutSecs: 5,
		Logger: getLogger("em2"),
	})
}

func TestLeaderSwitchWhenOldLeaderResigns(t *testing.T) {
	em1 := NewElectionManager(configs[0])
	em2 := NewElectionManager(configs[1])
	done := make(chan string)

	var s1, s2 func() <-chan error
	var err error

	//stop elections on method exit
	defer func() {
		if s1 != nil {
			<-s1()
		}
		if s2 != nil {
			<-s2()
		}
	}()

	//start election 1
	go func() {
		_, s1, err = em1.Run(func() error {
			done <- "em1"
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	//start election 2
	go func() {
		_, s2, err = em2.Run(func() error {
			done <- "em2"
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	l := waitForLeader(t, done)
	switch l {
	case "em1":
		em1.StopAndLog()
		expected := "em2"
		l := waitForLeader(t, done)
		if l != expected {
			t.Fatal(fmt.Sprintf("second leader: expected=%s, actual=%s", expected, l))
		}
		em2.StopAndLog()
	case "em2":
		em2.StopAndLog()
		expected := "em1"
		l := waitForLeader(t, done)
		if l != expected {
			t.Fatal(fmt.Sprintf("second leader: expected=%s, actual=%s", expected, l))
		}
		em1.StopAndLog()
	default:
		expected := "em1|em2"
		t.Fatal(fmt.Sprintf("first leader: expected=%s, actual=%s", expected, l))
	}
}

func TestErrorPropagationWhenLeaderRunnableFails(t *testing.T) {
	em1 := NewElectionManager(configs[0])
	em2 := NewElectionManager(configs[1])
	done := make(chan string)

	var s1, s2 func() <-chan error
	var err error
	var ec1 <-chan error

	defer func() {
		if s1 != nil {
			<-s1()
		}
		if s2 != nil {
			<-s2()
		}
	}()

	go func() {
		ec1, s1, err = em1.Run(func() error {
			done <- "em1"
			time.Sleep(5 * time.Second)
			return fmt.Errorf("em1: unexpected error")
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	l := waitForLeader(t, done)
	expected := "em1"
	if l != expected {
		t.Fatal(fmt.Sprintf("first leader: expected=%s, actual=%s", expected, l))
	}

	go func() {
		_, s2, err = em2.Run(func() error {
			done <- "em2"
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	//listen for error emitted by current leader
	lerr, lok := <-ec1
	if !lok || lerr == nil {
		t.Fatalf("expecting value on error channel of leader, got closed")
	}
	t.Log("error received in leader", lerr)

	l = waitForLeader(t, done)
	expected = "em2"
	if l != expected {
		t.Fatal(fmt.Sprintf("first leader: expected=%s, actual=%s", expected, l))
	}
	em2.StopAndLog()
}


func waitForLeader(t *testing.T, done <-chan string) string {
	l := <-done
	t.Log(l + " is leader")
	return l
}

func getLogger(val string) *zap.SugaredLogger {
	zapCfg := zap.NewProductionConfig()
	zapCfg.Level.SetLevel(zap.DebugLevel)
	zapCfg.DisableStacktrace = true
	zapLogger, err := zapCfg.Build(zap.Fields(zap.String("tracker", val)))
	if err != nil {
		log.Fatalf("err creating zap logger: %v", err)
	}
	return zapLogger.Sugar()
}