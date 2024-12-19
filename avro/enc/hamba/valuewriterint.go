package val2avro

// This file is generated using valwtr.tmpl. NEVER EDIT.

import (
	"context"
	"database/sql"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterIntNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.IntWriter = func(i int32) IO[Void] {
			return func(_ context.Context) (Void, error) {
				m[key] = i
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterIntNullNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.IntWriter = func(
			i sql.Null[int32],
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
