package val2avro

import (
	"context"

	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"
)

func ValueWriterNullNew() ValueWriterResolver {
	return func(m map[string]any, key string) sw.ValueWriter {
		var vw sw.ValueWriter = sw.ValueWriterDefault

		vw.PrimitiveWriter.NullWriter = func(_ struct{}) IO[Void] {
			return func(_ context.Context) (Void, error) {
				m[key] = nil
				return Empty, nil
			}
		}
		return vw
	}
}
