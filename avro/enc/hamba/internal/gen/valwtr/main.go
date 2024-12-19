package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"
)

var argLen int = len(os.Args)

var GetArgByIndex func(int) IO[string] = Lift(
	func(i int) (string, error) {
		if i < argLen {
			return os.Args[i], nil
		}
		return "", fmt.Errorf("invalid argument index: %v", i)
	},
)

// e.g, Int
var typeHint IO[string] = GetArgByIndex(1)

// e.g, int32
var primitive IO[string] = GetArgByIndex(2)

// e.g, valuewriterint.go
var filename IO[string] = Bind(
	typeHint,
	Lift(func(s string) (string, error) {
		var low string = strings.ToLower(s)
		return "valuewriter" + low + ".go", nil
	}),
)

type Config struct {
	TypeHint  string
	Primitive string
	Filename  string
}

var config IO[Config] = Bind(
	All(typeHint, primitive, filename),
	Lift(func(s []string) (Config, error) {
		return Config{
			TypeHint:  s[0],
			Primitive: s[1],
			Filename:  s[2],
		}, nil
	}),
)

var tmpl *template.Template = template.Must(
	template.ParseFiles("./internal/gen/valwtr/valwtr.tmpl"),
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
