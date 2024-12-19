package val2avro

// This file is generated using valwtr.tmpl. NEVER EDIT.

import (
	"context"
	"database/sql"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterDoubleNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.DoubleWriter = func(i float64) IO[Void] {
			return func(_ context.Context) (Void, error) {
				m[key] = i
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterDoubleNullNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.DoubleWriter = func(
			i sql.Null[float64],
		) IO[Void] {
			return func(_ context.Context) (Void, error) {
				switch i.Valid {
				case true:
					m[key] = i.V
				default:
					m[key] = nil
				}
				return Empty, nil
			}
		}
		return vw
	}
}
