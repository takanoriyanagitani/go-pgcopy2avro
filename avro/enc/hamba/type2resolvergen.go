package val2avro

// This file is generated using valwtr.tmpl. NEVER EDIT.

import (
	rs "github.com/takanoriyanagitani/go-rowlike2sqlvalues"
)

var type2resolverGen TypeToResolverGen = TypeToResolverGen{
	rs.PrimitiveString:  ValueWriterStringNew,
	rs.PrimitiveBytes:   ValueWriterBytesNew,
	rs.PrimitiveInt:     ValueWriterIntNew,
	rs.PrimitiveLong:    ValueWriterLongNew,
	rs.PrimitiveFloat:   ValueWriterFloatNew,
	rs.PrimitiveDouble:  ValueWriterDoubleNew,
	rs.PrimitiveBoolean: ValueWriterBooleanNew,
	rs.PrimitiveNull:    ValueWriterNullNew,
	rs.PrimitiveTime:    ValueWriterTimeNew,
	rs.PrimitiveUuid:    ValueWriterUuidNew,

	rs.NullString: ValueWriterStringNullNew,

	rs.NullBytes: ValueWriterBytesNullNew,

	rs.NullInt: ValueWriterIntNullNew,

	rs.NullLong: ValueWriterLongNullNew,

	rs.NullFloat: ValueWriterFloatNullNew,

	rs.NullDouble: ValueWriterDoubleNullNew,

	rs.NullBoolean: ValueWriterBooleanNullNew,

	rs.NullTime: ValueWriterTimeNullNew,

	rs.NullUuid: ValueWriterUuidNullNew,
}
