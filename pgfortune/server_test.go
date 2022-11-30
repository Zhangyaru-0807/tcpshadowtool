package main

import (
	"os/exec"
	"testing"
)

func TestPg(t *testing.T) {
	b := NewPgFortuneBackend(conn, func() ([]byte, error) {
		return exec.Command("sh", "-c", options.responseCommand).CombinedOutput()
	})
}
