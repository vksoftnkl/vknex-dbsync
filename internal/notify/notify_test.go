package notify

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stub returns a Notifier that records the subjects it would mail.
func stub(repeat time.Duration, sent *[]string) *Notifier {
	cfg := &config.Config{MailTo: []string{"to@example.com"}, MailFrom: "from@example.com", SMTPAddr: "localhost:25", MailRepeat: repeat, Interval: 5 * time.Minute}
	n := New(cfg, "", quiet)
	n.send = func(subject, _ string) error {
		*sent = append(*sent, subject)
		return nil
	}
	return n
}

func TestOncePerOutage(t *testing.T) {
	var sent []string
	n := stub(time.Hour, &sent)
	boom := errors.New("boom")
	n.Recovered("sync") // nothing failed yet
	n.Failed("sync", boom)
	n.Failed("sync", boom) // same outage
	n.Failed("instant sync", boom)
	n.Recovered("sync")
	n.Recovered("sync")
	n.Failed("sync", boom) // a new outage
	n.failing["sync"] = outage{since: time.Now().Add(-3 * time.Hour), mailed: time.Now().Add(-2 * time.Hour)}
	n.Failed("sync", boom) // -mail-repeat passed
	n.Failed("sync", boom)
	want := "sync failed,instant sync failed,sync recovered,sync failed,sync still failing"
	if got := strings.Join(sent, ","); got != want {
		t.Fatalf("mailed %q, want %q", got, want)
	}
}

func TestNoRepeat(t *testing.T) {
	var sent []string
	n := stub(0, &sent)
	n.Failed("sync", errors.New("boom"))
	n.failing["sync"] = outage{mailed: time.Now().Add(-48 * time.Hour)}
	n.Failed("sync", errors.New("boom"))
	if len(sent) != 1 {
		t.Fatalf("mailed %v, want one mail", sent)
	}
}

func TestNil(t *testing.T) {
	var n *Notifier
	n.Failed("sync", errors.New("boom"))
	n.Failed("sync", nil)
	n.Recovered("sync")
	n.Stopped("why")
	if err := n.Test(); err == nil {
		t.Fatal("Test on nil: no error")
	}
	if New(&config.Config{}, "", quiet) != nil || New(&config.Config{MailTo: []string{"a@b"}, DryRun: true}, "", quiet) != nil {
		t.Fatal("a Notifier without -mail-to or with -dry-run")
	}
}

// TestSMTP delivers through a tiny mail server: EHLO, AUTH, MAIL, RCPT, DATA, QUIT.
func TestSMTP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		c := textproto.NewConn(conn)
		c.PrintfLine("220 test ready")
		var msg strings.Builder
		for {
			line, err := c.ReadLine()
			if err != nil {
				return
			}
			switch strings.ToUpper(strings.SplitN(line, " ", 2)[0]) {
			case "EHLO":
				c.PrintfLine("250-test")
				c.PrintfLine("250 AUTH PLAIN")
			case "AUTH":
				c.PrintfLine("235 ok")
			case "MAIL", "RCPT":
				c.PrintfLine("250 ok")
			case "DATA":
				c.PrintfLine("354 go ahead")
				b, _ := c.ReadDotBytes()
				msg.Write(b)
				c.PrintfLine("250 queued")
			case "QUIT":
				c.PrintfLine("221 bye")
				got <- msg.String()
				return
			default:
				c.PrintfLine("500 unknown")
			}
		}
	}()

	cfg := &config.Config{
		MailTo: []string{"to@example.com", "two@example.com"}, MailFrom: "from@example.com",
		SMTPAddr: ln.Addr().String(), SMTPUser: "user", SMTPPassword: "secret",
		SourceURL: "postgres://u:p@src:5432/erp", TargetURL: "postgres://u:p@dst:5433/erp",
	}
	n := New(cfg, "sync.log", quiet)
	if err := n.Test(); err != nil {
		t.Fatal(err)
	}
	var msg string
	select {
	case msg = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no mail arrived")
	}
	for _, want := range []string{
		"From: from@example.com\n",
		"To: to@example.com, two@example.com\n",
		"Subject: [vknex-dbsync] test mail on " + n.host + "\n",
		"\nThe mail settings work",
		"\nu@src:5432/erp -> u@dst:5433/erp\nLog: sync.log\n",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("mail lacks %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, ":p@") {
		t.Errorf("mail shows the database password:\n%s", msg)
	}
}
