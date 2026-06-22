// Command wavespan-confdoc prints the documented tunables reference YAML to stdout. Regenerate the
// checked-in reference with:
//
//	go run ./cmd/wavespan-confdoc > config/reference.yaml
package main

import (
	"fmt"
	"os"

	"github.com/cwire/wavespan/internal/tunables"
)

func main() {
	if err := tunables.WriteReference(os.Stdout, tunables.Default()); err != nil {
		fmt.Fprintln(os.Stderr, "confdoc:", err)
		os.Exit(1)
	}
}
