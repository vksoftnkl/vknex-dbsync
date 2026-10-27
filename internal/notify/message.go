package notify

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"html/template"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"strings"
	"time"
)

// message is one mail before it is rendered. The same content goes out twice
// in one mail, as text and as HTML (multipart/alternative), so it reads well in
// every client: a styled card in Gmail or Outlook, plain lines in mutt.
type message struct {
	Tone    tone   // colours the HTML: red for a failure, green for a recovery, ...
	Event   string // the headline, e.g. "Sync failed"; the subject adds the host
	Summary string // one or two sentences under the headline
	Label   string // heading of Detail: "Error", "Panic"
	Detail  string // the error or the stack trace, in monospace; "" = none
	Facts   []fact // the details table; Host, Source, Target and Log are added

	written time.Time // set by Notifier.mail
	held    bool      // a send failed: it goes out late, and says so
}

// fact is one row of the details table.
type fact struct {
	Key, Value string
	Mono       bool // an address or a path: monospace in the HTML
}

func mono(key, value string) fact { return fact{Key: key, Value: value, Mono: true} }

// Font is the CSS for the value cell, chosen here so the template needs no
// branches inside a style attribute.
func (f fact) Font() template.CSS {
	if f.Mono {
		return "font-family:" + monoFont + ";font-size:13px;"
	}
	return "font-family:" + textFont + ";font-size:14px;"
}

// The font stacks name no quotes, so they survive attribute escaping, and
// start with fonts Outlook knows, which else falls back to Times New Roman.
const (
	textFont = "Segoe UI,Roboto,Helvetica,Arial,sans-serif"
	monoFont = "Menlo,Consolas,DejaVu Sans Mono,Liberation Mono,Courier New,monospace"
)

// tone is the badge text and the colour of an event. The colours are the
// status page's, dark enough for white text.
type tone struct{ Label, Color string }

var (
	toneFailed    = tone{"Failed", "#d03b3b"}
	toneFailing   = tone{"Still failing", "#b7501f"}
	toneRecovered = tone{"Recovered", "#15803d"}
	toneStopped   = tone{"Stopped", "#52514e"}
	toneCrashed   = tone{"Crashed", "#d03b3b"}
	toneTest      = tone{"Test", "#2f6fde"}
)

const subjectPrefix = "[vknex-dbsync] "

func (n *Notifier) subject(m *message) string {
	return subjectPrefix + m.Event + " on " + n.host
}

// facts puts the common rows around the event's own: the host first, where
// the data flows and the log file last.
func (n *Notifier) facts(m *message) []fact {
	all := append([]fact{{Key: "Host", Value: n.host}}, m.Facts...)
	if m.held {
		all = append(all, fact{Key: "Delayed", Value: "Written at " + stamp(m.written) + "; held back while the mail server could not be reached"})
	}
	all = append(all, mono("Source", n.source), mono("Target", n.target))
	if n.logFile != "" {
		all = append(all, mono("Log", n.logFile))
	}
	return all
}

// render builds the whole mail: the headers, then the text and the HTML part,
// each quoted-printable so that no line is too long for SMTP and UTF-8 passes
// any server. Line ends are CRLF throughout.
func (n *Notifier) render(m *message, now time.Time) ([]byte, error) {
	facts := n.facts(m)
	sent := stamp(now)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	from := mail.Address{Name: "VK Nex DB Sync", Address: n.cfg.MailFrom}
	headers := []string{
		"From: " + from.String(),
		"To: " + strings.Join(n.cfg.MailTo, ", "),
		"Subject: " + mime.QEncoding.Encode("utf-8", n.subject(m)),
		"Date: " + now.Format(time.RFC1123Z),
		"Message-ID: " + messageID(now, n.cfg.MailFrom),
		"Auto-Submitted: auto-generated", // RFC 3834: no out-of-office replies
		"MIME-Version: 1.0",
		`Content-Type: multipart/alternative; boundary="` + mw.Boundary() + `"`,
		"",
		"",
	}
	buf.WriteString(strings.Join(headers, "\r\n"))
	if err := part(mw, "text/plain", n.text(m, facts, sent)); err != nil {
		return nil, err
	}
	html, err := n.html(m, facts, sent)
	if err != nil {
		return nil, err
	}
	if err := part(mw, "text/html", html); err != nil {
		return nil, err
	}
	if err := mw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// part adds body to mw as one quoted-printable UTF-8 part of type ctype.
func part(mw *multipart.Writer, ctype, body string) error {
	h := textproto.MIMEHeader{}
	h.Set("Content-Type", ctype+"; charset=utf-8")
	h.Set("Content-Transfer-Encoding", "quoted-printable")
	w, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	qp := quotedprintable.NewWriter(w)
	if _, err := io.WriteString(qp, body); err != nil {
		return err
	}
	return qp.Close()
}

// messageID is unique per mail, so that threads and duplicates sort out in
// the client. The domain is the sender's.
func messageID(now time.Time, from string) string {
	var r [6]byte
	rand.Read(r[:])
	domain := from[strings.LastIndex(from, "@")+1:]
	return fmt.Sprintf("<%d.%s@%s>", now.UnixNano(), hex.EncodeToString(r[:]), domain)
}

// text is the plain version: a title, the summary wrapped at 72 columns, the
// detail, the facts aligned in two columns and a signature.
func (n *Notifier) text(m *message, facts []fact, sent string) string {
	var b strings.Builder
	title := m.Event + " on " + n.host
	fmt.Fprintf(&b, "%s\n%s\n\n", title, strings.Repeat("=", len(title)))
	fmt.Fprintf(&b, "%s\n", wrap(m.Summary, 72))
	if m.Detail != "" {
		fmt.Fprintf(&b, "\n%s\n%s\n", m.Label, strings.Repeat("-", len(m.Label)))
		fmt.Fprintf(&b, "%s\n", strings.TrimRight(m.Detail, "\n"))
	}
	b.WriteString("\n")
	width := 0
	for _, f := range facts {
		width = max(width, len(f.Key))
	}
	for _, f := range facts {
		fmt.Fprintf(&b, "%-*s  %s\n", width, f.Key, f.Value)
	}
	fmt.Fprintf(&b, "\n-- \nSent by vknex-dbsync on %s at %s\n", n.host, sent)
	return b.String()
}

// wrap breaks s at spaces into lines of at most width characters.
func wrap(s string, width int) string {
	var b strings.Builder
	col := 0
	for _, word := range strings.Fields(s) {
		switch {
		case col > 0 && col+1+len(word) > width:
			b.WriteByte('\n')
			col = 0
		case col > 0:
			b.WriteByte(' ')
			col++
		}
		b.WriteString(word)
		col += len(word)
	}
	return b.String()
}

// html is the styled version: a card with a coloured band and badge, the
// headline, the summary, the detail in a monospace box, the facts as a table.
// Styles are inline and the layout is tables, which is what mail clients
// render; html/template escapes the texts.
func (n *Notifier) html(m *message, facts []fact, sent string) (string, error) {
	var b strings.Builder
	err := page.Execute(&b, struct {
		Tone                                      tone
		Event, Summary, Label, Detail, Host, Sent string
		Facts                                     []fact
	}{m.Tone, m.Event, m.Summary, m.Label, m.Detail, n.host, sent, facts})
	return b.String(), err
}

// The page is written the way mail clients render reliably: tables for the
// layout, every style inline and repeated per cell, colours as bgcolor
// attributes as well as background-color (classic Outlook ignores the
// background shorthand and padding on spans). The style block only serves
// clients that honour prefers-color-scheme (Apple Mail, Thunderbird); Gmail
// strips it and recolours on its own in dark mode.
var page = template.Must(template.New("mail").Parse(strings.NewReplacer(
	"$TEXT", textFont, "$MONO", monoFont).Replace(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="x-apple-disable-message-reformatting">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<title>{{.Event}} on {{.Host}}</title>
<style>
:root{color-scheme:light dark;supported-color-schemes:light dark}
@media (prefers-color-scheme:dark){
.bg{background-color:#0d0d0d!important}
.card{background-color:#1a1a19!important;border-color:#2c2c2a!important}
.ink{color:#f0efec!important}
.ink2{color:#c3c2b7!important}
.box{background-color:#111110!important;border-color:#2c2c2a!important}
.row{border-color:#2c2c2a!important}
}
</style>
</head>
<body class="bg" bgcolor="#f4f4f1" style="margin:0;padding:0;background-color:#f4f4f1;">
<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">{{.Summary}}</div>
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" class="bg" bgcolor="#f4f4f1" style="background-color:#f4f4f1;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellspacing="0" cellpadding="0" border="0" class="card" bgcolor="#ffffff" style="width:100%;max-width:600px;background-color:#ffffff;border:1px solid #e1e0d9;border-radius:12px;">
<tr><td height="6" bgcolor="{{.Tone.Color}}" style="height:6px;line-height:6px;font-size:1px;background-color:{{.Tone.Color}};border-radius:12px 12px 0 0;">&nbsp;</td></tr>
<tr><td style="padding:22px 28px 4px 28px;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
<tr>
<td style="font-family:$TEXT;font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:#898781;font-weight:600;">VK Nex DB Sync &nbsp;&middot;&nbsp; {{.Host}}</td>
<td align="right">
<table role="presentation" cellspacing="0" cellpadding="0" border="0" align="right">
<tr><td bgcolor="{{.Tone.Color}}" style="font-family:$TEXT;font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:#ffffff;background-color:{{.Tone.Color}};padding:4px 10px;border-radius:999px;white-space:nowrap;">{{.Tone.Label}}</td></tr>
</table>
</td>
</tr>
</table>
<h1 class="ink" style="margin:16px 0 8px 0;font-family:$TEXT;font-size:22px;line-height:1.25;font-weight:700;color:#0b0b0b;">{{.Event}}</h1>
<p class="ink2" style="margin:0;font-family:$TEXT;font-size:15px;line-height:1.55;color:#52514e;">{{.Summary}}</p>
</td></tr>
{{if .Detail}}<tr><td style="padding:18px 28px 0 28px;">
<div style="font-family:$TEXT;font-size:12px;letter-spacing:.04em;text-transform:uppercase;color:#898781;font-weight:600;margin-bottom:6px;">{{.Label}}</div>
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
<tr>
<td width="4" bgcolor="{{.Tone.Color}}" style="width:4px;background-color:{{.Tone.Color}};font-size:1px;line-height:1px;border-radius:8px 0 0 8px;">&nbsp;</td>
<td class="box" bgcolor="#f6f6f4" style="background-color:#f6f6f4;border:1px solid #e1e0d9;border-left:0;border-radius:0 8px 8px 0;padding:12px 14px;">
<pre class="ink" style="margin:0;font-family:$MONO;font-size:12.5px;line-height:1.5;color:#0b0b0b;white-space:pre-wrap;word-wrap:break-word;word-break:break-word;">{{.Detail}}</pre>
</td>
</tr>
</table>
</td></tr>
{{end}}<tr><td style="padding:20px 28px 6px 28px;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" class="row" style="border-top:1px solid #e1e0d9;">
{{range .Facts}}<tr>
<td class="row" valign="top" width="130" style="padding:9px 16px 9px 0;border-bottom:1px solid #eeede8;font-family:$TEXT;font-size:12px;letter-spacing:.04em;text-transform:uppercase;color:#898781;font-weight:600;white-space:nowrap;">{{.Key}}</td>
<td class="row ink" valign="top" style="padding:9px 0;border-bottom:1px solid #eeede8;color:#0b0b0b;line-height:1.45;word-break:break-word;{{.Font}}">{{.Value}}</td>
</tr>
{{end}}</table>
</td></tr>
<tr><td style="padding:14px 28px 22px 28px;font-family:$TEXT;font-size:12px;line-height:1.5;color:#898781;">Sent by vknex-dbsync on {{.Host}} at {{.Sent}}.</td></tr>
</table>
</td></tr>
</table>
</body>
</html>
`)))
