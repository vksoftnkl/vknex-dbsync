package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadMail(t *testing.T) {
	load := func(extra ...string) (*Config, error) {
		return Load(append([]string{"-source", "postgres://a/x", "-target", "postgres://b/x"}, extra...))
	}
	c, err := load("-mail-to", " Prathap <p@example.com>, q@example.com ", "-smtp", "smtp.example.com:587", "-smtp-user", "s@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.MailTo, ","); got != "p@example.com,q@example.com" || c.MailFrom != "s@example.com" || c.MailRepeat != time.Hour {
		t.Fatalf("got to=%q from=%q repeat=%s", got, c.MailFrom, c.MailRepeat)
	}
	if c, err := load(); err != nil || len(c.MailTo) != 0 {
		t.Fatalf("without -mail-to: to=%v err=%v", c.MailTo, err)
	}
	for name, args := range map[string][]string{
		"no smtp":      {"-mail-to", "p@example.com"},
		"no port":      {"-mail-to", "p@example.com", "-smtp", "smtp.example.com", "-smtp-user", "s@example.com"},
		"no from":      {"-mail-to", "p@example.com", "-smtp", "smtp.example.com:587"},
		"from no addr": {"-mail-to", "p@example.com", "-smtp", "smtp.example.com:587", "-smtp-user", "apikey"},
		"bad to":       {"-mail-to", "nobody", "-smtp", "smtp.example.com:587", "-mail-from", "s@example.com"},
		"test no to":   {"-mail-test"},
	} {
		if _, err := load(args...); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestParseKindIntervals(t *testing.T) {
	got, err := parseKindIntervals(" log:1h, Audit : 24h ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["LOG"] != time.Hour || got["AUDIT"] != 24*time.Hour {
		t.Fatalf("got %v", got)
	}
	for _, bad := range []string{"LOG", "LOG:", ":1h", "LOG:1H", "LOG:0s", "LOG:-5m"} {
		if _, err := parseKindIntervals(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}
