package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"

	pp "github.com/takanoriyanagitani/go-rowlike2sqlvalues/rowlike/rdb/postgresql/pgcopy"
	ph "github.com/takanoriyanagitani/go-rowlike2sqlvalues/rowlike/rdb/postgresql/pgcopy/header"

	eh "github.com/takanoriyanagitani/go-pgcopy2avro/avro/enc/hamba"
)

var EnvVarByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var schemaFilename IO[string] = EnvVarByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()
			var buf strings.Builder
			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}
			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		}
	}
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var columnInfoMap IO[map[int16]pp.ColumnInfo] = Bind(
	schemaContent,
	Lift(eh.ColumnInfoMapFromSchema),
)

var typ2resolverGen eh.TypeToResolverGen = eh.Type2resolverGen
var key2valwtrResolver IO[eh.KeyToValueWriterResolver] = Bind(
	columnInfoMap,
	Lift(func(cm map[int16]pp.ColumnInfo) (eh.KeyToValueWriterResolver, error) {
		return typ2resolverGen.ColInfoToValueWriterResolver(cm), nil
	}),
)

var pgheader IO[ph.PgcopySimpleHeader] = ph.HeaderFromStdinDefault
var pgrows IO[iter.Seq2[pp.PgRow, error]] = Bind(
	pgheader,
	func(_ ph.PgcopySimpleHeader) IO[iter.Seq2[pp.PgRow, error]] {
		return pp.PgRowsFromStdin
	},
)

var values IO[iter.Seq2[map[string]sw.Value, error]] = Bind(
	columnInfoMap,
	func(cm map[int16]pp.ColumnInfo) IO[iter.Seq2[map[string]sw.Value, error]] {
		return Bind(
			pgrows,
			pp.ColumnMapToPgRows(cm),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	values,
	func(
		original iter.Seq2[map[string]sw.Value, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			key2valwtrResolver,
			Curry(eh.MapsToMaps)(original),
		)
	},
)

var pgcopy2maps2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return pgcopy2maps2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
