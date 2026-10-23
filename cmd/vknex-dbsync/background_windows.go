package main

import (
	"syscall"
	"unsafe"
)

var (
	createMutex = syscall.NewLazyDLL("kernel32.dll").NewProc("CreateMutexW")
	messageBox  = syscall.NewLazyDLL("user32.dll").NewProc("MessageBoxW")
)

// alreadyRunning creates a named mutex that lives as long as this process, so
// a second double-click finds the first copy instead of starting another.
func alreadyRunning() bool {
	name, _ := syscall.UTF16PtrFromString(`Local\vknex-dbsync`)
	h, _, err := createMutex.Call(0, 0, uintptr(unsafe.Pointer(name)))
	return h != 0 && err == syscall.ERROR_ALREADY_EXISTS
}

// alert shows msg in a message box.
func alert(msg string) {
	const mbIconWarning = 0x30
	text, _ := syscall.UTF16PtrFromString(msg)
	title, _ := syscall.UTF16PtrFromString("vknex-dbsync")
	messageBox.Call(0, uintptr(unsafe.Pointer(text)), uintptr(unsafe.Pointer(title)), mbIconWarning)
}
