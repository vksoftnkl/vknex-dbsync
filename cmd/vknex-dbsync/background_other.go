//go:build !windows

package main

// alreadyRunning is Windows-only; elsewhere the service manager runs one copy.
func alreadyRunning() bool { return false }

// alert is Windows-only; fatal has already printed msg to stderr.
func alert(string) {}
