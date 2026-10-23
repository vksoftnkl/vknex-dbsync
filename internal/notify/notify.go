// Package notify mails the -mail-to addresses when a sync fails, works again
// or the program stops.
package notify

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

// timeout bounds the whole SMTP conversation: a hanging mail server must not
// hold up the next run or the exit.
const timeout = 30 * time.Second

// Notifier sends the mails. A nil Notifier (no -mail-to, or -dry-run) sends
// nothing, so callers never check for one.
type Notifier struct {
	cfg    *config.Config
	log    *slog.Logger
	host   string                           // this machine, named in every mail
	footer string                           // source -> target and the log file, under every mail
	send   func(subject, body string) error // smtpSend; a stub in tests

	mu      sync.Mutex
	failing map[string]outage // what -> its outage, while it keeps failing
}

type outage struct{ since, mailed time.Time }

// New returns the Notifier for cfg, or nil when nothing is to be mailed.
// logFile is where the logs go, named in the mails (empty: the console).
func New(cfg *config.Config, logFile string, log *slog.Logger) *Notifier {
	if len(cfg.MailTo) == 0 || cfg.DryRun {
		return nil
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown host"
	}
	n := &Notifier{cfg: cfg, log: log, host: host, failing: map[string]outage{}}
	n.footer = pg.Describe(cfg.SourceURL) + " -> " + pg.Describe(cfg.TargetURL)
	if logFile != "" {
		n.footer += "\nLog: " + logFile
	}
	n.send = n.smtpSend
	return n
}

// Failed mails that what (e.g. "sync") failed with err: once when it starts
// failing, then every -mail-repeat while it keeps failing. Without that a
// dead tunnel would mail on every retry.
func (n *Notifier) Failed(what string, err error) {
	if n == nil {
		return
	}
	now := time.Now()
	n.mu.Lock()
	o, failing := n.failing[what]
	if failing && (n.cfg.MailRepeat <= 0 || now.Sub(o.mailed) < n.cfg.MailRepeat) {
		n.mu.Unlock()
		return
	}
	if !failing {
		o.since = now
	}
	o.mailed = now
	n.failing[what] = o
	n.mu.Unlock()

	subject := what + " failed"
	text := fmt.Sprintf("The %s on %s failed at %s.", what, n.host, stamp(now))
	if failing {
		subject = what + " still failing"
		text = fmt.Sprintf("The %s on %s has been failing since %s, last at %s.", what, n.host, stamp(o.since), stamp(now))
	}
	if n.cfg.Interval > 0 {
		text += " It is retried; you get a mail when it works again"
		if n.cfg.MailRepeat > 0 {
			text += fmt.Sprintf(", and one every %s while it keeps failing", n.cfg.MailRepeat)
		}
		text += "."
	}
	n.mail(subject, fmt.Sprintf("%s\n\n%v", text, err))
}

// Recovered mails that what works again, if Failed mailed about it.
func (n *Notifier) Recovered(what string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	o, failing := n.failing[what]
	delete(n.failing, what)
	n.mu.Unlock()
	if !failing {
		return
	}
	n.mail(what+" recovered", fmt.Sprintf("The %s on %s works again since %s, after failing since %s.", what, n.host, stamp(time.Now()), stamp(o.since)))
}

// Stopped mails that the program ended; why says how, in full sentences.
func (n *Notifier) Stopped(why string) {
	if n == nil {
		return
	}
	n.mail("stopped", fmt.Sprintf("vknex-dbsync on %s stopped at %s.\n\n%s", n.host, stamp(time.Now()), why))
}

// Test sends a test mail, for -mail-test.
func (n *Notifier) Test() error {
	if n == nil {
		return errors.New("nothing to mail: -mail-to is empty or -dry-run is set")
	}
	return n.send("test mail", "The mail settings work: this is the test mail of -mail-test.")
}

func (n *Notifier) mail(subject, body string) {
	if err := n.send(subject, body); err != nil {
		n.log.Error("could not send the mail", "subject", subject, "err", err)
		return
	}
	n.log.Info("mail sent", "subject", subject, "to", strings.Join(n.cfg.MailTo, ","))
}

func stamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05 MST")
}

// smtpSend delivers one mail through -smtp: TLS from the start on port 465,
// otherwise STARTTLS when the server offers it, logged in with -smtp-user.
func (n *Notifier) smtpSend(subject, body string) error {
	cfg := n.cfg
	host, port, err := net.SplitHostPort(cfg.SMTPAddr)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: timeout}
	tlsCfg := &tls.Config{ServerName: host}
	var conn net.Conn
	if port == "465" || port == "smtps" {
		conn, err = tls.DialWithDialer(dialer, "tcp", cfg.SMTPAddr, tlsCfg)
	} else {
		conn, err = dialer.Dial("tcp", cfg.SMTPAddr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(tlsCfg); err != nil {
			return err
		}
	}
	if cfg.SMTPUser != "" { // refuses to send the password over a plain connection, except to localhost
		if err := c.Auth(smtp.PlainAuth("", cfg.SMTPUser, cfg.SMTPPassword, host)); err != nil {
			return err
		}
	}
	if err := c.Mail(cfg.MailFrom); err != nil {
		return err
	}
	for _, to := range cfg.MailTo {
		if err := c.Rcpt(to); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(n.message(subject, body)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

// message builds the mail: headers, the body and the footer, with CRLF line ends.
func (n *Notifier) message(subject, body string) []byte {
	lines := []string{
		"From: " + n.cfg.MailFrom,
		"To: " + strings.Join(n.cfg.MailTo, ", "),
		"Subject: [vknex-dbsync] " + subject + " on " + n.host,
		"Date: " + time.Now().Format(time.RFC1123Z),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"Content-Transfer-Encoding: 8bit",
		"",
		body,
		"",
		n.footer,
		"",
	}
	text := strings.ReplaceAll(strings.Join(lines, "\n"), "\r\n", "\n") // an error text may already have CRLF
	return []byte(strings.ReplaceAll(text, "\n", "\r\n"))
}
