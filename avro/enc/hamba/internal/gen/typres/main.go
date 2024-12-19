package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"text/template"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"
)

var filename IO[string] = Of("type2resolvergen.go")

type Config struct {
	TypeHints []string
	Filename  string
}

var config IO[Config] = Bind(
	filename,
	Lift(func(s string) (Config, error) {
		return Config{
			TypeHints: []string{
				"String",
				"Bytes",
				"Int",
				"Long",
				"Float",
				"Double",
				"Boolean",
				"Null",
				"Time",
				"Uuid",
			},
			Filename: s,
		}, nil
	}),
)

var tmpl *template.Template = template.Must(
	template.ParseFiles("./internal/gen/typres/typres.tmpl"),
)

func TemplateToWriter(
	t *template.Template,
	w io.Writer,
	cfg Config,
) error {
	var bw *bufio.Writer = bufio.NewWriter(w)
	defer bw.Flush()
	return t.Execute(bw, cfg)
}

func TemplateToFileLike(
	t *template.Template,
	file io.WriteCloser,
	cfg Config,
) error {
	defer file.Close()
	return TemplateToWriter(t, file, cfg)
}

func TemplateToFilename(
	t *template.Template,
	cfg Config,
) error {
	var filename string = cfg.Filename
	f, e := os.Create(filename)
	if nil != e {
		return e
	}
	return TemplateToFileLike(t, f, cfg)
}

func ConfigToTemplateToFilename(
	cfg Config,
) IO[Void] {
	return func(_ context.Context) (Void, error) {
		return Empty, TemplateToFilename(
			tmpl,
			cfg,
		)
	}
}

var tmpl2file IO[Void] = Bind(
	config,
	ConfigToTemplateToFilename,
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return tmpl2file(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		panic(e)
	}
}
