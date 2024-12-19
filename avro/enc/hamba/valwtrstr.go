package val2avro

import (
	"context"
	"database/sql"
	"strings"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterStringNew() ValueWriterResolver {
	var buf strings.Builder
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.StringWriter = func(i string) IO[Void] {
			return func(_ context.Context) (Void, error) {
				buf.Reset()
				_, _ = buf.WriteString(i) // error is always nil or OOM
				m[key] = buf.String()
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterStringNullNew() ValueWriterResolver {
	var buf strings.Builder
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.StringWriter = func(
			i sql.Null[string],
		) IO[Void] {
			return func(_ context.Context) (Void, error) {
				if !i.Valid {
					m[key] = nil
					return Empty, nil
				}

				buf.Reset()
				_, _ = buf.WriteString(i.V) // error is always nil or OOM
				m[key] = buf.String()
				return Empty, nil
			}
		}
		return vw
	}
}
