package val2avro

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterUuidNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.UuidWriter = func(i uuid.UUID) IO[Void] {
			return func(_ context.Context) (Void, error) {
				m[key] = i
				return Empty, nil
			}
		}
		return vw
	}
}

func ValueWriterUuidNullNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.NullableWriter.UuidWriter = func(
			i sql.Null[uuid.UUID],
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
