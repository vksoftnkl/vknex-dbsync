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
	"net/textproto"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
)

const (
	// timeout bounds the whole SMTP conversation: a hanging mail server must
	// not hold up the next run or the exit.
	timeout = 30 * time.Second

	// A mail the server cannot take waits, and goes out with the next mail or
	// after retryUnsent: a sync fails most often when the network is down,
	// and so does the mail about it.
	retryUnsent = time.Minute
	maxUnsent   = 50 // beyond this the oldest waiting mail is dropped
)

// Notifier sends the mails. A nil Notifier (no -mail-to, or -dry-run) sends
// nothing, so callers never check for one.
type Notifier struct {
	cfg     *config.Config
	log     *slog.Logger
	host    string               // this machine, named in every mail
	source  string               // user@host:port/db of the source, in every mail
	target  string               // the same for the target
	logFile string               // where the logs go, in every mail; "" = the console
	send    func(*message) error // smtpSend; a stub in tests
	retryIn time.Duration        // retryUnsent; shorter in tests

	sending sync.Mutex // one flush at a time, so the mails leave in order

	mu       sync.Mutex
	failing  map[string]outage // what -> its outage, while it keeps failing
	unsent   []*message        // waiting for the mail server, oldest first
	retrying bool              // a flush is due after retryIn
	stopping bool              // Stopped or Crashed: no later flush will come
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
	n := &Notifier{
		cfg: cfg, log: log, host: host, logFile: logFile,
		source: pg.Describe(cfg.SourceURL), target: pg.Describe(cfg.TargetURL),
		failing: map[string]outage{}, retryIn: retryUnsent,
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

	retry := ""
	if n.cfg.Interval > 0 {
		retry = " It is retried; you get a mail when it works again"
		if n.cfg.MailRepeat > 0 {
			retry += fmt.Sprintf(", and one every %s while it keeps failing", human(n.cfg.MailRepeat))
		}
		retry += "."
	}
	m := &message{Tone: toneFailed, Event: capital(what) + " failed", Label: "Error", Detail: fmt.Sprint(err)}
	if failing {
		m.Tone, m.Event = toneFailing, capital(what)+" still failing"
		m.Summary = fmt.Sprintf("The %s on %s has been failing for %s, since %s. The last attempt failed at %s.%s",
			what, n.host, human(now.Sub(o.since)), stamp(o.since), stamp(now), retry)
		m.Facts = []fact{{Key: "Failing since", Value: stamp(o.since)}, {Key: "Last failure", Value: stamp(now)}, {Key: "For", Value: human(now.Sub(o.since))}}
	} else {
		m.Summary = fmt.Sprintf("The %s on %s failed at %s.%s", what, n.host, stamp(now), retry)
		m.Facts = []fact{{Key: "Failed at", Value: stamp(now)}}
	}
	n.mail(m)
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
	now := time.Now()
	n.mail(&message{
		Tone:  toneRecovered,
		Event: capital(what) + " recovered",
		Summary: fmt.Sprintf("The %s on %s works again since %s. It had been failing for %s, since %s.",
			what, n.host, stamp(now), human(now.Sub(o.since)), stamp(o.since)),
		Facts: []fact{{Key: "Works since", Value: stamp(now)}, {Key: "Failed since", Value: stamp(o.since)}, {Key: "Outage", Value: human(now.Sub(o.since))}},
	})
}

// Stopped mails that the program ended; why says how, in full sentences.
func (n *Notifier) Stopped(why string) {
	if n == nil {
		return
	}
	n.stop()
	now := time.Now()
	n.mail(&message{
		Tone:    toneStopped,
		Event:   "Stopped",
		Summary: fmt.Sprintf("vknex-dbsync on %s stopped at %s. %s", n.host, stamp(now), why),
		Facts:   []fact{{Key: "Stopped at", Value: stamp(now)}},
	})
}

// Crashed mails that the program ended with the panic value r and its stack.
func (n *Notifier) Crashed(r any, stack string) {
	if n == nil {
		return
	}
	n.stop()
	now := time.Now()
	n.mail(&message{
		Tone:  toneCrashed,
		Event: "Crashed",
		Summary: fmt.Sprintf("vknex-dbsync on %s crashed at %s. On Linux the service starts it again after 30 s; "+
			"on Windows start it again by hand. Nothing is synced until then.", n.host, stamp(now)),
		Label:  "Panic",
		Detail: fmt.Sprintf("panic: %v\n\n%s", r, stack),
		Facts:  []fact{{Key: "Crashed at", Value: stamp(now)}},
	})
}

// Test sends a test mail, for -mail-test.
func (n *Notifier) Test() error {
	if n == nil {
		return errors.New("nothing to mail: -mail-to is empty or -dry-run is set")
	}
	return n.send(&message{
		Tone:  toneTest,
		Event: "Test mail",
		Summary: "The mail settings work: this is the test mail of -mail-test. " +
			"A mail like this one comes when a sync fails, when it works again and when the program stops.",
		Facts: []fact{{Key: "Sent at", Value: stamp(time.Now())}, {Key: "Mail to", Value: strings.Join(n.cfg.MailTo, ", ")}},
	})
}

// mail sends m after the mails still waiting for the mail server.
func (n *Notifier) mail(m *message) {
	m.written = time.Now()
	n.mu.Lock()
	if len(n.unsent) >= maxUnsent {
		old := n.unsent[0]
		n.unsent = n.unsent[1:]
		n.log.Warn("mail dropped: too many mails wait for the mail server", "subject", old.Event, "written", stamp(old.written))
	}
	n.unsent = append(n.unsent, m)
	n.mu.Unlock()
	n.flush(m)
}

// flush sends the waiting mails, oldest first, until the mail server cannot
// be reached; then they wait for the next mail or retryIn. added is the mail
// just written, nil for a retry: only a new mail logs that it has to wait, so
// a long outage logs once per mail, not once per retry.
func (n *Notifier) flush(added *message) {
	n.sending.Lock()
	defer n.sending.Unlock()
	to := strings.Join(n.cfg.MailTo, ",")
	for {
		n.mu.Lock()
		if len(n.unsent) == 0 {
			n.mu.Unlock()
			return
		}
		m := n.unsent[0]
		n.mu.Unlock()

		err := n.send(m)
		if err != nil && !refused(err) {
			n.wait(added, err)
			return
		}
		n.mu.Lock()
		n.unsent = slices.DeleteFunc(n.unsent, func(w *message) bool { return w == m }) // mail may have dropped it meanwhile
		n.mu.Unlock()
		switch {
		case err != nil:
			n.log.Error("could not send the mail: the mail server refused it", "subject", m.Event, "err", err)
		case m.held:
			n.log.Info("mail sent", "subject", m.Event, "to", to, "late", human(time.Since(m.written)))
		default:
			n.log.Info("mail sent", "subject", m.Event, "to", to)
		}
	}
}

// wait keeps the unsent mails for a later flush after err, or drops them when
// none will come: the program stops, or it runs once (no -interval).
func (n *Notifier) wait(added *message, err error) {
	n.mu.Lock()
	if n.stopping || n.cfg.Interval <= 0 {
		lost := make([]string, len(n.unsent))
		for i, w := range n.unsent {
			lost[i] = w.Event
		}
		n.unsent = nil
		n.mu.Unlock()
		n.log.Error("could not send the mail", "subjects", strings.Join(lost, ","), "err", err)
		return
	}
	for _, w := range n.unsent {
		w.held = true
	}
	waiting := len(n.unsent)
	if !n.retrying {
		n.retrying = true
		time.AfterFunc(n.retryIn, func() {
			n.mu.Lock()
			n.retrying = false
			n.mu.Unlock()
			n.flush(nil)
		})
	}
	n.mu.Unlock()
	if added != nil {
		n.log.Error("could not send the mail; it is sent when the mail server can be reached",
			"subject", added.Event, "waiting", waiting, "retry_in", n.retryIn.String(), "err", err)
	}
}

// stop tells flush that no retry will come: the mails go now or never.
func (n *Notifier) stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopping = true
}

// refused reports whether the mail server answered and turned the mail down
// (SMTP 5xx: a wrong password, an unknown sender): sending it again would not help.
func refused(err error) bool {
	var te *textproto.Error
	return errors.As(err, &te) && te.Code >= 500
}

func stamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05 MST")
}

// human writes d for a sentence: 2h 5m, 4m 30s, 12s.
func human(d time.Duration) string {
	if d < time.Second {
		return "under a second"
	}
	if d >= time.Hour {
		d = d.Round(time.Minute)
		h, m := int(d.Hours()), int(d.Minutes())%60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	}
	d = d.Round(time.Second)
	if d >= time.Minute {
		m, s := int(d.Minutes()), int(d.Seconds())%60
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

// capital upper-cases the first letter: "sync" -> "Sync", for a headline.
func capital(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// smtpSend delivers one mail through -smtp: TLS from the start on port 465,
// otherwise STARTTLS when the server offers it, logged in with -smtp-user.
func (n *Notifier) smtpSend(m *message) error {
	cfg := n.cfg
	msg, err := n.render(m, time.Now())
	if err != nil {
		return err
	}
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
	if _, err := w.Write(msg); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
