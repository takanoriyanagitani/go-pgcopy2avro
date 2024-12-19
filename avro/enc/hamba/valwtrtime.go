package val2avro

import (
	"context"
	"database/sql"
	"time"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterTimeNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.TimeWriter = func(i time.Time) IO[Void] {
			return func(_ context.Context) (Void, error) {
				m[key] = i
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterTimeNullNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.TimeWriter = func(
			i sql.Null[time.Time],
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
