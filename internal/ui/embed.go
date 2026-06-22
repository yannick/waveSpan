// Package ui embeds the built Vite SPA into the node binary (design/26) so it ships inside the
// FROM scratch image with no external files. `make ui` runs `vite build` into ./dist before
// `make build`; a committed placeholder keeps `go build` working before the first frontend build.
package ui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// Assets returns the embedded SPA file system rooted at the build output.
func Assets() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err) // dist is always embedded (placeholder committed)
	}
	return sub
}
