/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kudu;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.truncateEpochMicrosToMillis;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public final class TypeHelper
{
    private TypeHelper() {}

    public static org.apache.kudu.Type toKuduClientType(Type type)
    {
        if (type == BooleanType.BOOLEAN) {
            return org.apache.kudu.Type.BOOL;
        }
        if (type == TinyintType.TINYINT) {
            return org.apache.kudu.Type.INT8;
        }
        if (type == SmallintType.SMALLINT) {
            return org.apache.kudu.Type.INT16;
        }
        if (type == IntegerType.INTEGER) {
            return org.apache.kudu.Type.INT32;
        }
        if (type == BigintType.BIGINT) {
            return org.apache.kudu.Type.INT64;
        }
        if (type == RealType.REAL) {
            return org.apache.kudu.Type.FLOAT;
        }
        if (type == DoubleType.DOUBLE) {
            return org.apache.kudu.Type.DOUBLE;
        }
        if (type instanceof DecimalType) {
            return org.apache.kudu.Type.DECIMAL;
        }
        if (type instanceof CharType) {
            return org.apache.kudu.Type.STRING;
        }
        if (type instanceof VarcharType) {
            return org.apache.kudu.Type.STRING;
        }
        if (type instanceof VarbinaryType) {
            return org.apache.kudu.Type.BINARY;
        }
        if (type == DateType.DATE) {
            return org.apache.kudu.Type.DATE;
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return org.apache.kudu.Type.UNIXTIME_MICROS;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
    }

    public static Type fromKuduColumn(ColumnSchema column)
    {
        return fromKuduClientType(column.getType(), column.getTypeAttributes());
    }

    private static Type fromKuduClientType(org.apache.kudu.Type ktype, ColumnTypeAttributes attributes)
    {
        switch (ktype) {
            case BOOL:
                return BooleanType.BOOLEAN;
            case INT8:
                return TinyintType.TINYINT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case DECIMAL:
                return DecimalType.createDecimalType(attributes.getPrecision(), attributes.getScale());
            case STRING:
                return VarcharType.VARCHAR;
            case BINARY:
                return VarbinaryType.VARBINARY;
            case DATE:
                return DateType.DATE;
            case UNIXTIME_MICROS:
                return TIMESTAMP_MILLIS;
            // TODO: add support for varchar types: https://github.com/trinodb/trino/issues/11009
            case VARCHAR:
                break;
        }
        throw new IllegalStateException("Kudu type not implemented for " + ktype);
    }

    public static Object getJavaValue(Type type, Object nativeValue)
    {
        if (type == BooleanType.BOOLEAN) {
            return nativeValue;
        }
        if (type == TinyintType.TINYINT) {
            return ((Long) nativeValue).byteValue();
        }
        if (type == SmallintType.SMALLINT) {
            return ((Long) nativeValue).shortValue();
        }
        if (type == IntegerType.INTEGER) {
            return ((Long) nativeValue).intValue();
        }
        if (type == BigintType.BIGINT) {
            return nativeValue;
        }
        if (type == RealType.REAL) {
            // conversion can result in precision lost
            return intBitsToFloat(((Long) nativeValue).intValue());
        }
        if (type == DoubleType.DOUBLE) {
            return nativeValue;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return new BigDecimal(BigInteger.valueOf((long) nativeValue), decimalType.getScale());
            }
            return new BigDecimal(((Int128) nativeValue).toBigInteger(), decimalType.getScale());
        }
        if (type instanceof VarcharType) {
            return ((Slice) nativeValue).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return ((Slice) nativeValue).toByteBuffer();
        }
        if (type.equals(DateType.DATE)) {
            return nativeValue;
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            // Kudu's native format is in microseconds
            return nativeValue;
        }
        throw new IllegalStateException("Back conversion not implemented for " + type);
    }

    public static Object getObject(Type type, RowResult row, int field)
    {
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return Decimals.encodeScaledValue(row.getDecimal(field), decimalType.getScale());
        }
        throw new IllegalStateException("getObject not implemented for " + type);
    }

    public static long getLong(Type type, RowResult row, int field)
    {
        if (type == TinyintType.TINYINT) {
            return row.getByte(field);
        }
        if (type == SmallintType.SMALLINT) {
            return row.getShort(field);
        }
        if (type == IntegerType.INTEGER) {
            return row.getInt(field);
        }
        if (type == BigintType.BIGINT) {
            return row.getLong(field);
        }
        if (type == RealType.REAL) {
            return floatToRawIntBits(row.getFloat(field));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return row.getDecimal(field).unscaledValue().longValue();
            }
            throw new IllegalStateException("getLong not supported for long decimal: " + type);
        }
        if (type.equals(DateType.DATE)) {
            return row.getInt(field);
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return truncateEpochMicrosToMillis(row.getLong(field));
        }
        throw new IllegalStateException("getLong not implemented for " + type);
    }

    public static boolean getBoolean(Type type, RowResult row, int field)
    {
        if (type == BooleanType.BOOLEAN) {
            return row.getBoolean(field);
        }
        throw new IllegalStateException("getBoolean not implemented for " + type);
    }

    public static double getDouble(Type type, RowResult row, int field)
    {
        if (type == DoubleType.DOUBLE) {
            return row.getDouble(field);
        }
        throw new IllegalStateException("getDouble not implemented for " + type);
    }

    public static Slice getSlice(Type type, RowResult row, int field)
    {
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(row.getString(field));
        }
        if (type instanceof VarbinaryType) {
            return Slices.wrappedHeapBuffer(row.getBinary(field));
        }
        throw new IllegalStateException("getSlice not implemented for " + type);
    }
}
