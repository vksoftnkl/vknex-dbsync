package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDotEnv(t *testing.T) {
	dir := t.TempDir()
	if err := loadDotEnv(filepath.Join(dir, "missing.env")); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	long := filepath.Join(dir, "long.env")
	if err := os.WriteFile(long, []byte("VKNEX_TEST_LONG="+strings.Repeat("x", 70000)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadDotEnv(long); err == nil {
		t.Error("line over 64 KiB: no error")
	}
}

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

func TestLoadClient(t *testing.T) {
	load := func(extra ...string) (*Config, error) {
		return Load(append([]string{"-source", "postgres://a/x", "-target", "postgres://b/x"}, extra...))
	}
	c, err := load("-client-name", "  SRI LAKSHMI TIMBERS ")
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientName != "SRI LAKSHMI TIMBERS" || c.ClientDB != "vknex_clients" {
		t.Fatalf("got name=%q db=%q", c.ClientName, c.ClientDB)
	}
	if c, err := load(); err != nil || c.ClientName != "" {
		t.Fatalf("without -client-name: name=%q err=%v", c.ClientName, err)
	}
	if _, err := load("-client-name", "x", "-client-db", " "); err == nil {
		t.Error("empty -client-db: no error")
	}
}

func TestLoadTwoWay(t *testing.T) {
	load := func(extra ...string) (*Config, error) {
		return Load(append([]string{"-source", "postgres://a/x", "-target", "postgres://b/x"}, extra...))
	}
	c, err := load("-two-way", "-mark-synced", "-two-way-kinds", " master, setting ", "-two-way-wins", " Live ")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.TwoWayKinds, ","); !c.TwoWay || got != "MASTER,SETTING" || c.TwoWayWins != "live" {
		t.Fatalf("got two-way=%v kinds=%q wins=%q", c.TwoWay, got, c.TwoWayWins)
	}
	if c, err := load(); err != nil || c.TwoWay || c.TwoWayWins != "local" || strings.Join(c.TwoWayKinds, ",") != "MASTER" {
		t.Fatalf("defaults: two-way=%v kinds=%v wins=%q err=%v", c.TwoWay, c.TwoWayKinds, c.TwoWayWins, err)
	}
	for name, args := range map[string][]string{
		"no mark-synced":  {"-two-way"},
		"no table master": {"-two-way", "-mark-synced", "-table-master", ""},
		"no kinds":        {"-two-way", "-mark-synced", "-two-way-kinds", " , "},
		"bad wins":        {"-two-way-wins", "newest"},
	} {
		if _, err := load(args...); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
