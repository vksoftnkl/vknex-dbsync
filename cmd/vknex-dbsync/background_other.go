//go:build !windows

package main

import "os/exec"

// alreadyRunning is Windows-only; elsewhere the service manager runs one copy.
func alreadyRunning() bool { return false }

// alert is Windows-only; fatal has already printed msg to stderr.
func alert(string) {}

// openBrowser opens url in the default browser. On Linux the desktop icon
// (deploy/linux/control.sh open) does this; it is here for completeness.
func openBrowser(url string) error { return exec.Command("xdg-open", url).Start() }
