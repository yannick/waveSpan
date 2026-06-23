package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yannick/wavespan/internal/backup"
	"github.com/yannick/wavespan/internal/storage"
)

// backupCmd runs a node-local backup of a wavesdb storage directory to a file. It operates directly
// on storage (run against a stopped node or a snapshot dir); ANN vector indexes are rebuilt on
// restore, never backed up.
func backupCmd(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	storagePath := fs.String("storage", "", "wavesdb storage directory")
	out := fs.String("out", "", "destination backup file")
	includeVec := fs.Bool("include-vector-indexes", false, "(no-op; ANN indexes are rebuilt on restore)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *storagePath == "" || *out == "" {
		return fmt.Errorf("usage: wavespanctl backup --storage <dir> --out <file>")
	}
	store, err := storage.OpenWavesdb(*storagePath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	man, err := backup.Backup(store, f, *includeVec)
	if err != nil {
		return err
	}
	fmt.Printf("backed up %d records to %s\n", man.Entries, *out)
	return nil
}

// restoreCmd restores a backup file into a fresh wavesdb storage directory.
func restoreCmd(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	storagePath := fs.String("storage", "", "fresh wavesdb storage directory to restore into")
	in := fs.String("in", "", "source backup file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *storagePath == "" || *in == "" {
		return fmt.Errorf("usage: wavespanctl restore --storage <dir> --in <file>")
	}
	store, err := storage.OpenWavesdb(*storagePath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	f, err := os.Open(*in)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	man, err := backup.Restore(store, f)
	if err != nil {
		return err
	}
	fmt.Printf("restored %d records into %s (vector ANN indexes rebuild on node startup)\n", man.Entries, *storagePath)
	return nil
}
