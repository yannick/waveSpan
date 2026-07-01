// Command wavespan-snapshot is the OFFLINE storage tool: it dumps/restores a single wavesdb storage
// directory to/from a file, operating directly on storage (run against a stopped node or a copied
// snapshot dir). ANN vector indexes are rebuilt on restore, never stored.
//
// It links the server's storage engine, so it is a separate binary from wavespanctl (which is built on
// the SDK — the two proto stub sets cannot be linked into one binary). For online, cluster-wide,
// consistent backups to an object store, use `wavespanctl backup`.
//
//	wavespan-snapshot dump    --storage <dir> --out <file>
//	wavespan-snapshot restore --storage <dir> --in  <file>
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yannick/wavespan/internal/backup"
	"github.com/yannick/wavespan/internal/storage"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wavespan-snapshot:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("no command")
	}
	switch args[0] {
	case "dump":
		return dumpCmd(args[1:])
	case "restore":
		return restoreCmd(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func dumpCmd(args []string) error {
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	storagePath := fs.String("storage", "", "wavesdb storage directory")
	out := fs.String("out", "", "destination snapshot file")
	includeVec := fs.Bool("include-vector-indexes", false, "(no-op; ANN indexes are rebuilt on restore)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *storagePath == "" || *out == "" {
		return fmt.Errorf("usage: wavespan-snapshot dump --storage <dir> --out <file>")
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
	fmt.Printf("dumped %d records to %s\n", man.Entries, *out)
	return nil
}

func restoreCmd(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	storagePath := fs.String("storage", "", "fresh wavesdb storage directory to restore into")
	in := fs.String("in", "", "source snapshot file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *storagePath == "" || *in == "" {
		return fmt.Errorf("usage: wavespan-snapshot restore --storage <dir> --in <file>")
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

func usage() {
	fmt.Fprint(os.Stderr, `wavespan-snapshot — offline wavesdb storage dump/restore

usage:
  wavespan-snapshot dump    --storage <dir> --out <file>
  wavespan-snapshot restore --storage <dir> --in  <file>

Run against a stopped node or a copied storage dir. For online cluster backups, use `+"`wavespanctl backup`"+`.
`)
}
