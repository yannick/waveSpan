// Command wavespanctl is the admin/client CLI. At M0 it is a stub supporting `version`;
// subcommands that talk to AdminService/ObservabilityService over Connect arrive in M3+.
package main

import (
	"fmt"
	"os"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.0.0-dev"

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "version":
		fmt.Printf("wavespanctl %s\n", version)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: wavespanctl <command>\n\ncommands:\n  version    print the client version")
}
