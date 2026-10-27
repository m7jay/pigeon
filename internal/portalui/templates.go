package portalui

import (
	"embed"
	"io/fs"
)

//go:embed templates
var templatesFS embed.FS

// TemplateFS is the embedded shared template tree, rooted at "templates". It
// holds the base layout and the reusable components; each portal adds its own
// templates on top of a Clone of Base().
var TemplateFS fs.FS = templatesFS
