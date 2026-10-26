package notify

import (
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/mail"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stub returns a Notifier that records the headlines it would mail.
func stub(repeat time.Duration, sent *[]string) *Notifier {
	cfg := &config.Config{MailTo: []string{"to@example.com"}, MailFrom: "from@example.com", SMTPAddr: "localhost:25", MailRepeat: repeat, Interval: 5 * time.Minute}
	n := New(cfg, "", quiet)
	n.send = func(m *message) error {
		*sent = append(*sent, m.Event)
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
	want := "Sync failed,Instant sync failed,Sync recovered,Sync failed,Sync still failing"
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
	n.Crashed("boom", "stack")
	if err := n.Test(); err == nil {
		t.Fatal("Test on nil: no error")
	}
	if New(&config.Config{}, "", quiet) != nil || New(&config.Config{MailTo: []string{"a@b"}, DryRun: true}, "", quiet) != nil {
		t.Fatal("a Notifier without -mail-to or with -dry-run")
	}
}

func TestHuman(t *testing.T) {
	for d, want := range map[time.Duration]string{
		300 * time.Millisecond: "under a second",
		12 * time.Second:       "12s",
		4*time.Minute + 30*time.Second + 400*time.Millisecond: "4m 30s",
		59*time.Second + 800*time.Millisecond:                 "1m",
		time.Hour:                                             "1h",
		2*time.Hour + 5*time.Minute + 29*time.Second:          "2h 5m",
		26 * time.Hour:                                        "26h",
	} {
		if got := human(d); got != want {
			t.Errorf("human(%s) = %q, want %q", d, got, want)
		}
	}
}

// parts decodes a rendered mail: its headers and the text and HTML bodies.
func parts(t *testing.T, raw string) (hdr mail.Header, text, html string) {
	t.Helper()
	msg, err := mail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("not a mail: %v\n%s", err, raw)
	}
	mt, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil || mt != "multipart/alternative" {
		t.Fatalf("Content-Type %q, want multipart/alternative", msg.Header.Get("Content-Type"))
	}
	if n := strings.Count(raw, "Content-Transfer-Encoding: quoted-printable"); n != 2 {
		t.Errorf("%d quoted-printable parts, want 2", n)
	}
	mr := multipart.NewReader(msg.Body, params["boundary"])
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(p) // the reader decodes quoted-printable itself
		if err != nil {
			t.Fatal(err)
		}
		body := strings.ReplaceAll(string(b), "\r\n", "\n")
		switch ct := p.Header.Get("Content-Type"); ct {
		case "text/plain; charset=utf-8":
			text = body
		case "text/html; charset=utf-8":
			html = body
		default:
			t.Errorf("unexpected part %q", ct)
		}
	}
	if text == "" || html == "" {
		t.Fatalf("mail lacks a text or an HTML part:\n%s", raw)
	}
	return msg.Header, text, html
}

// TestRender checks the whole mail: headers, both bodies, escaping and that
// no line is too long for SMTP.
func TestRender(t *testing.T) {
	cfg := &config.Config{
		MailTo: []string{"to@example.com", "two@example.com"}, MailFrom: "from@example.com",
		SMTPAddr: "localhost:25", SourceURL: "postgres://u:p@src:5432/erp", TargetURL: "postgres://u:p@dst:5433/erp",
	}
	n := New(cfg, "/var/log/sync.log", quiet)
	n.host = "erp-vps"
	long := strings.Repeat("x", 200)
	m := &message{Tone: toneFailed, Event: "Sync failed", Label: "Error",
		Summary: "The sync on erp-vps failed. " + strings.Repeat("word ", 40),
		Detail:  "connect <target>: a & b\n\tsecond line " + long,
		Facts:   []fact{{Key: "Failed at", Value: "2026-10-24 10:23:42 IST"}}}
	now := time.Date(2026, 10, 24, 10, 23, 42, 0, time.FixedZone("IST", 5*3600+1800))
	raw, err := n.render(m, now)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if strings.Contains(strings.ReplaceAll(s, "\r\n", ""), "\n") {
		t.Error("a bare LF in the mail")
	}
	for i, line := range strings.Split(s, "\r\n") {
		if len(line) > 998 {
			t.Errorf("line %d has %d bytes", i+1, len(line))
		}
	}
	hdr, text, html := parts(t, s)
	for k, want := range map[string]string{
		"From":           `"VK Nex DB Sync" <from@example.com>`,
		"To":             "to@example.com, two@example.com",
		"Subject":        "[vknex-dbsync] Sync failed on erp-vps",
		"Date":           "Sat, 24 Oct 2026 10:23:42 +0530",
		"Auto-Submitted": "auto-generated",
		"MIME-Version":   "1.0",
	} {
		if got := hdr.Get(k); got != want {
			t.Errorf("%s: %q, want %q", k, got, want)
		}
	}
	if id := hdr.Get("Message-ID"); !strings.HasPrefix(id, "<") || !strings.HasSuffix(id, "@example.com>") {
		t.Errorf("Message-ID %q", id)
	}
	for _, want := range []string{
		"Sync failed on erp-vps\n======================\n\nThe sync on erp-vps failed.",
		"\nError\n-----\nconnect <target>: a & b\n\tsecond line " + long + "\n",
		"\nHost       erp-vps\nFailed at  2026-10-24 10:23:42 IST\nSource     u@src:5432/erp\nTarget     u@dst:5433/erp\nLog        /var/log/sync.log\n",
		"\n-- \nSent by vknex-dbsync on erp-vps at 2026-10-24 10:23:42 IST\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "word") && len(line) > 72 {
			t.Errorf("summary not wrapped: %q", line)
		}
	}
	for _, want := range []string{
		">Sync failed</h1>", ">Failed</td>", `bgcolor="#d03b3b"`, "background-color:#d03b3b",
		"connect &lt;target&gt;: a &amp; b", ">Error</div>",
		">Failed at</td>", ">u@src:5432/erp</td>", ">u@dst:5433/erp</td>", ">/var/log/sync.log</td>",
		"font-family:Menlo,Consolas,DejaVu Sans Mono,Liberation Mono,Courier New,monospace;font-size:13px;", "Sent by vknex-dbsync on erp-vps at 2026-10-24 10:23:42 IST.",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("HTML lacks %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "<target>") {
		t.Error("HTML has the error text unescaped")
	}
	if strings.Contains(s, ":p@") {
		t.Errorf("mail shows the database password:\n%s", s)
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
	hdr, text, html := parts(t, msg) // ReadDotBytes turned CRLF into LF; mail.ReadMessage takes both
	if got, want := hdr.Get("Subject"), "[vknex-dbsync] Test mail on "+n.host; got != want {
		t.Errorf("Subject %q, want %q", got, want)
	}
	if got, want := hdr.Get("To"), "to@example.com, two@example.com"; got != want {
		t.Errorf("To %q, want %q", got, want)
	}
	for _, want := range []string{"The mail settings work", "u@src:5432/erp", "u@dst:5433/erp", "sync.log", "to@example.com, two@example.com"} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
		if !strings.Contains(html, want) {
			t.Errorf("HTML lacks %q:\n%s", want, html)
		}
	}
	if strings.Contains(msg, ":p@") {
		t.Errorf("mail shows the database password:\n%s", msg)
	}
}
