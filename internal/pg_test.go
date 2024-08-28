package internal

import (
	"testing"
)

func TestConnect(t *testing.T) {
	db, err := Connect()
	if err != nil {
		t.Fail()
	}

	defer db.Close()
}
