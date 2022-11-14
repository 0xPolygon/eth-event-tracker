package storeboltdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	eventtracker "github.com/0xPolygon/eth-event-tracker"
)

func TestInMemoryStore(t *testing.T) {
	eventtracker.TestStore(t, func(t *testing.T) (eventtracker.Entry, func()) {
		return setupDB(t)
	})
}

func setupDB(t *testing.T) (eventtracker.Entry, func()) {
	dir, err := ioutil.TempDir("/tmp", "boltdb-test")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test.db")
	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	close := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	entry, err := store.GetEntry("")
	if err != nil {
		t.Fatal(err)
	}
	return entry, close
}
