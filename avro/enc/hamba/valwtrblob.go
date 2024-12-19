package val2avro

import (
	"bytes"
	"context"
	"database/sql"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterBytesNew() ValueWriterResolver {
	var buf bytes.Buffer
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.BytesWriter = func(i []byte) IO[Void] {
			return func(_ context.Context) (Void, error) {
				buf.Reset()
				_, _ = buf.Write(i) // error is always nil or panic
				m[key] = buf.Bytes()
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterBytesNullNew() ValueWriterResolver {
	var buf bytes.Buffer
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.BytesWriter = func(
			i sql.Null[[]byte],
		) IO[Void] {
			return func(_ context.Context) (Void, error) {
				if !i.Valid {
					m[key] = nil
					return Empty, nil
				}

				buf.Reset()
				_, _ = buf.Write(i.V) // error is always nil or panic
				m[key] = buf.Bytes()
				return Empty, nil
			}
		}
		return vw
	}
}
