package benchui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

func spaFS() fs.FS { sub, _ := fs.Sub(distFS, "dist"); return sub }
