package val2avro

import (
	"context"
	"errors"
	"iter"
	"maps"

	rs "github.com/takanoriyanagitani/go-rowlike2sqlvalues"
	. "github.com/takanoriyanagitani/go-rowlike2sqlvalues/util"

	sw "github.com/takanoriyanagitani/go-rowlike2sqlvalues/writer"

	pp "github.com/takanoriyanagitani/go-rowlike2sqlvalues/rowlike/rdb/postgresql/pgcopy"

	ha "github.com/hamba/avro/v2"
)

var (
	ErrInvalidSchema error = errors.New("invalid schema")
)

type TypeConverter func(ha.Type) rs.PrimitiveType

var type2primMap map[ha.Type]rs.PrimitiveType = map[ha.Type]rs.PrimitiveType{
	ha.String:  rs.PrimitiveString,
	ha.Bytes:   rs.PrimitiveBytes,
	ha.Int:     rs.PrimitiveInt,
	ha.Long:    rs.PrimitiveLong,
	ha.Float:   rs.PrimitiveFloat,
	ha.Double:  rs.PrimitiveDouble,
	ha.Boolean: rs.PrimitiveBoolean,
	ha.Null:    rs.PrimitiveNull,
}

var logicalMap map[ha.LogicalType]rs.PrimitiveType = map[ha.LogicalType]rs.
	PrimitiveType{
	ha.UUID:            rs.PrimitiveUuid,
	ha.TimestampMicros: rs.PrimitiveTime,
	ha.Date:            rs.PrimitiveUnknown, // todo
	ha.TimeMicros:      rs.PrimitiveUnknown, // todo
	ha.Duration:        rs.PrimitiveUnknown, // todo
}

var nullLogicalMap map[ha.LogicalType]rs.PrimitiveType = map[ha.LogicalType]rs.
	PrimitiveType{
	ha.UUID:            rs.NullUuid,
	ha.TimestampMicros: rs.NullTime,
	ha.Date:            rs.PrimitiveUnknown, // todo
	ha.TimeMicros:      rs.PrimitiveUnknown, // todo
	ha.Duration:        rs.PrimitiveUnknown, // todo
}

var null2primMap map[ha.Type]rs.PrimitiveType = map[ha.Type]rs.PrimitiveType{
	ha.String:  rs.NullString,
	ha.Bytes:   rs.NullBytes,
	ha.Int:     rs.NullInt,
	ha.Long:    rs.NullLong,
	ha.Float:   rs.NullFloat,
	ha.Double:  rs.NullDouble,
	ha.Boolean: rs.NullBoolean,
}

func logicalToType(
	m map[ha.LogicalType]rs.PrimitiveType,
	typ ha.LogicalType,
) rs.PrimitiveType {
	mapd, found := m[typ]
	switch found {
	case true:
		return mapd
	default:
		return rs.PrimitiveUnknown
	}
}

var LogicalToType func(ha.LogicalType) rs.PrimitiveType = Curry(
	logicalToType,
)(logicalMap)

var NullableLogicalToType func(ha.LogicalType) rs.PrimitiveType = Curry(
	logicalToType,
)(nullLogicalMap)

func UnionToPrimitiveType(u *ha.UnionSchema) rs.PrimitiveType {
	var ret rs.PrimitiveType = rs.PrimitiveUnknown
	var schemas []ha.Schema = u.Types()
	for _, schema := range schemas {
		switch t := schema.(type) {
		case *ha.PrimitiveSchema:
			var ls ha.LogicalSchema = t.Logical()
			if nil != ls {
				return NullableLogicalToType(ls.Type())
			}
		default:
		}

		var typ ha.Type = schema.Type()
		mapd, found := null2primMap[typ]
		if found {
			return mapd
		}
	}
	return ret
}

func FixedToPrimitiveType(u *ha.FixedSchema) rs.PrimitiveType {
	var ret rs.PrimitiveType = rs.PrimitiveUnknown
	var logical ha.LogicalSchema = u.Logical()
	var size int = u.Size()
	if nil == logical {
		switch size {
		case 16:
			return LogicalToType(ha.UUID)
		default:
			return ret
		}
	}
	var ltyp ha.LogicalType = logical.Type()
	return LogicalToType(ltyp)
}

func PrimitiveToType(p *ha.PrimitiveSchema) rs.PrimitiveType {
	var logical ha.LogicalSchema = p.Logical()
	if nil != logical {
		var logicalType ha.LogicalType = logical.Type()
		return LogicalToType(logicalType)
	}
	var typ ha.Type = p.Type()
	mapd, found := type2primMap[typ]
	switch found {
	case true:
		return mapd
	default:
		return rs.PrimitiveUnknown
	}
}

func SchemaToType(s ha.Schema) rs.PrimitiveType {
	switch t := s.(type) {
	case *ha.PrimitiveSchema:
		return PrimitiveToType(t)
	case *ha.UnionSchema:
		return UnionToPrimitiveType(t)
	case *ha.FixedSchema:
		return FixedToPrimitiveType(t)
	default:
		return rs.PrimitiveUnknown
	}
}

func FieldsToMap(
	fields []*ha.Field,
) (map[int16]pp.ColumnInfo, error) {
	var pairs iter.Seq2[int16, pp.ColumnInfo] = func(
		yield func(int16, pp.ColumnInfo) bool,
	) {
		for i, field := range fields {
			var s ha.Schema = field.Type()
			var typ rs.PrimitiveType = SchemaToType(s)
			var name string = field.Name()
			var ix int16 = int16(i)
			ci := pp.ColumnInfo{
				Name:          name,
				PrimitiveType: typ,
			}
			yield(ix, ci)
		}
	}
	return maps.Collect(pairs), nil
}

func RecordSchemaToMap(
	s *ha.RecordSchema,
) (map[int16]pp.ColumnInfo, error) {
	var fields []*ha.Field = s.Fields()
	return FieldsToMap(fields)
}

func ColumnInfoMapFromSchemaHamba(
	s ha.Schema,
) (map[int16]pp.ColumnInfo, error) {
	switch t := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToMap(t)
	default:
		return map[int16]pp.ColumnInfo{}, ErrInvalidSchema
	}
}

func ColumnInfoMapFromSchema(
	schema string,
) (map[int16]pp.ColumnInfo, error) {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return map[int16]pp.ColumnInfo{}, e
	}
	return ColumnInfoMapFromSchemaHamba(parsed)
}

//go:generate go run internal/gen/valwtr/main.go Int int32
//go:generate go run internal/gen/valwtr/main.go Long int64
//go:generate go run internal/gen/valwtr/main.go Float float32
//go:generate go run internal/gen/valwtr/main.go Double float64
//go:generate go run internal/gen/valwtr/main.go Boolean bool
//go:generate gofmt -s -w .
type ValueWriterResolver func(map[string]any, string) sw.ValueWriter

var ValueWriterResolverDefault ValueWriterResolver = func(
	_ map[string]any,
	_ string,
) sw.ValueWriter {
	return sw.ValueWriterDefault
}

type KeyToValueWriterResolver func(string) ValueWriterResolver

func ValuesToWriter(
	out map[string]any,
	m map[string]sw.Value,
	key2vwResolver KeyToValueWriterResolver,
) IO[Void] {
	return func(ctx context.Context) (Void, error) {
		clear(out)
		for key, val := range m {
			var vwr ValueWriterResolver = key2vwResolver(key)
			var wtr sw.ValueWriter = vwr(out, key)
			_, e := val(wtr)(ctx)
			if nil != e {
				return Empty, e
			}
		}
		return Empty, nil
	}
}

func MapsToMaps(
	original iter.Seq2[map[string]sw.Value, error],
	key2vwResolver KeyToValueWriterResolver,
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}
			for m, e := range original {
				clear(buf)
				if nil != e {
					if !yield(buf, e) {
						return
					}
					continue
				}

				_, e = ValuesToWriter(
					buf,
					m,
					key2vwResolver,
				)(ctx)
				if !yield(buf, e) {
					return
				}
			}
		}, nil
	}
}

//go:generate go run internal/gen/typres/main.go
//go:generate gofmt -s -w .
type TypeToResolverGen map[rs.PrimitiveType]func() ValueWriterResolver

var Type2resolverGen TypeToResolverGen = type2resolverGen

func (g TypeToResolverGen) ColInfoToKeyToValWtrMap(
	colinfo map[int16]pp.ColumnInfo,
) map[string]ValueWriterResolver {
	ret := map[string]ValueWriterResolver{}
	for _, val := range colinfo {
		var name string = val.Name
		var typ rs.PrimitiveType = val.PrimitiveType
		rgen, found := g[typ]
		if !found {
			continue
		}
		var resolver ValueWriterResolver = rgen()
		ret[name] = resolver
	}
	return ret
}

func (g TypeToResolverGen) ColInfoToValueWriterResolver(
	colinfo map[int16]pp.ColumnInfo,
) KeyToValueWriterResolver {
	var m map[string]ValueWriterResolver = g.ColInfoToKeyToValWtrMap(colinfo)
	return func(key string) ValueWriterResolver {
		res, found := m[key]
		switch found {
		case true:
			return res
		default:
			return ValueWriterResolverDefault
		}
	}
}
