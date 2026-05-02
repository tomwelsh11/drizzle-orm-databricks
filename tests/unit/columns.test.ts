import { describe, expect, it } from 'vitest';
import { entityKind, is } from 'drizzle-orm/entity';
import {
  DatabricksBigInt,
  DatabricksBigIntBuilder,
  DatabricksBinary,
  DatabricksBinaryBuilder,
  DatabricksBoolean,
  DatabricksBooleanBuilder,
  DatabricksChar,
  DatabricksCharBuilder,
  DatabricksColumn,
  DatabricksColumnBuilder,
  DatabricksDate,
  DatabricksDateBuilder,
  DatabricksDecimal,
  DatabricksDecimalBuilder,
  DatabricksDouble,
  DatabricksDoubleBuilder,
  DatabricksFloat,
  DatabricksFloatBuilder,
  DatabricksInt,
  DatabricksIntBuilder,
  DatabricksSmallInt,
  DatabricksSmallIntBuilder,
  DatabricksString,
  DatabricksStringBuilder,
  DatabricksTimestamp,
  DatabricksTimestampBuilder,
  DatabricksTinyInt,
  DatabricksTinyIntBuilder,
  DatabricksVarChar,
  DatabricksVarCharBuilder,
  DatabricksVariant,
  DatabricksVariantBuilder,
  bigint,
  binary,
  boolean,
  char,
  date,
  decimal,
  double,
  float,
  int,
  smallint,
  string,
  timestamp,
  tinyint,
  varchar,
  variant,
} from '../../src/columns/index';
import { timestampNtz } from '../../src/columns/timestamp';
import { databricksTable } from '../../src/table';

// Helper to build a column from a builder so we can test column-level behavior
// (including overridden mapFromDriverValue/mapToDriverValue and getSQLType).
function buildColumn<B extends DatabricksColumnBuilder>(
  builder: B,
  name = 'col',
): DatabricksColumn {
  (builder as any).setName(name);
  // Use a fresh table per call so columns don't leak unique-name state.
  const table = databricksTable('t', { _placeholder: int() }) as any;
  return builder.build(table);
}

describe('string()', () => {
  it('creates a DatabricksStringBuilder', () => {
    const b = string();
    expect(b).toBeInstanceOf(DatabricksStringBuilder);
  });

  it('builds a DatabricksString column with correct SQL type', () => {
    const col = buildColumn(string());
    expect(col).toBeInstanceOf(DatabricksString);
    expect(col.getSQLType()).toBe('STRING');
  });

  it('has correct entityKind on builder and column', () => {
    expect((DatabricksStringBuilder as any)[entityKind]).toBe('DatabricksStringBuilder');
    expect((DatabricksString as any)[entityKind]).toBe('DatabricksString');
  });

  it('supports notNull(), default(), primaryKey(), unique() chain', () => {
    const b = string()
      .notNull()
      .default('hello')
      .primaryKey()
      .unique('uniq_name');
    expect((b as any).config.notNull).toBe(true);
    expect((b as any).config.hasDefault).toBe(true);
    expect((b as any).config.default).toBe('hello');
    expect((b as any).config.primaryKey).toBe(true);
    expect((b as any).config.isUnique).toBe(true);
    expect((b as any).config.uniqueName).toBe('uniq_name');
  });
});

describe('varchar()', () => {
  it('creates a DatabricksVarCharBuilder', () => {
    expect(varchar({ length: 255 })).toBeInstanceOf(DatabricksVarCharBuilder);
  });

  it('renders VARCHAR(N) with the given length', () => {
    const col = buildColumn(varchar({ length: 255 }));
    expect(col).toBeInstanceOf(DatabricksVarChar);
    expect(col.getSQLType()).toBe('VARCHAR(255)');
  });

  it('accepts a column name as the first argument', () => {
    const col = buildColumn(varchar('email', { length: 64 }), 'email');
    expect(col.getSQLType()).toBe('VARCHAR(64)');
  });

  it('exposes enum values when configured', () => {
    const col = buildColumn(varchar({ length: 16, enum: ['a', 'b'] })) as DatabricksVarChar;
    expect(col.enumValues).toEqual(['a', 'b']);
  });

  it('has correct entityKind on builder and column', () => {
    expect((DatabricksVarCharBuilder as any)[entityKind]).toBe('DatabricksVarCharBuilder');
    expect((DatabricksVarChar as any)[entityKind]).toBe('DatabricksVarChar');
  });
});

describe('char()', () => {
  it('creates a DatabricksCharBuilder', () => {
    expect(char({ length: 10 })).toBeInstanceOf(DatabricksCharBuilder);
  });

  it('renders CHAR(N) with the given length', () => {
    const col = buildColumn(char({ length: 10 }));
    expect(col).toBeInstanceOf(DatabricksChar);
    expect(col.getSQLType()).toBe('CHAR(10)');
  });

  it('has correct entityKind on builder and column', () => {
    expect((DatabricksCharBuilder as any)[entityKind]).toBe('DatabricksCharBuilder');
    expect((DatabricksChar as any)[entityKind]).toBe('DatabricksChar');
  });
});

describe('int()', () => {
  it('creates a DatabricksIntBuilder', () => {
    expect(int()).toBeInstanceOf(DatabricksIntBuilder);
  });

  it('builds an INT column', () => {
    const col = buildColumn(int());
    expect(col).toBeInstanceOf(DatabricksInt);
    expect(col.getSQLType()).toBe('INT');
  });

  it('has correct entityKind on builder and column', () => {
    expect((DatabricksIntBuilder as any)[entityKind]).toBe('DatabricksIntBuilder');
    expect((DatabricksInt as any)[entityKind]).toBe('DatabricksInt');
  });

  it('supports primaryKey() and notNull()', () => {
    const b = int().primaryKey().notNull();
    expect((b as any).config.primaryKey).toBe(true);
    expect((b as any).config.notNull).toBe(true);
  });
});

describe('bigint()', () => {
  it('creates a DatabricksBigIntBuilder', () => {
    expect(bigint()).toBeInstanceOf(DatabricksBigIntBuilder);
  });

  it('builds a BIGINT column', () => {
    const col = buildColumn(bigint());
    expect(col).toBeInstanceOf(DatabricksBigInt);
    expect(col.getSQLType()).toBe('BIGINT');
  });

  it('has correct entityKind on builder and column', () => {
    expect((DatabricksBigIntBuilder as any)[entityKind]).toBe('DatabricksBigIntBuilder');
    expect((DatabricksBigInt as any)[entityKind]).toBe('DatabricksBigInt');
  });

  it('mapFromDriverValue returns a bigint for number, string, and bigint inputs', () => {
    const col = buildColumn(bigint()) as DatabricksBigInt;
    expect(col.mapFromDriverValue(42)).toBe(42n);
    expect(col.mapFromDriverValue('123')).toBe(123n);
    expect(col.mapFromDriverValue(7n)).toBe(7n);
    expect(typeof col.mapFromDriverValue(1)).toBe('bigint');
  });

  it('mapToDriverValue serialises bigints as strings', () => {
    const col = buildColumn(bigint()) as DatabricksBigInt;
    expect(col.mapToDriverValue(123n)).toBe('123');
    expect(col.mapToDriverValue(0n)).toBe('0');
  });
});

describe('smallint()', () => {
  it('creates a DatabricksSmallIntBuilder and builds a SMALLINT column', () => {
    expect(smallint()).toBeInstanceOf(DatabricksSmallIntBuilder);
    const col = buildColumn(smallint());
    expect(col).toBeInstanceOf(DatabricksSmallInt);
    expect(col.getSQLType()).toBe('SMALLINT');
  });

  it('has correct entityKind', () => {
    expect((DatabricksSmallIntBuilder as any)[entityKind]).toBe('DatabricksSmallIntBuilder');
    expect((DatabricksSmallInt as any)[entityKind]).toBe('DatabricksSmallInt');
  });
});

describe('tinyint()', () => {
  it('creates a DatabricksTinyIntBuilder and builds a TINYINT column', () => {
    expect(tinyint()).toBeInstanceOf(DatabricksTinyIntBuilder);
    const col = buildColumn(tinyint());
    expect(col).toBeInstanceOf(DatabricksTinyInt);
    expect(col.getSQLType()).toBe('TINYINT');
  });

  it('has correct entityKind', () => {
    expect((DatabricksTinyIntBuilder as any)[entityKind]).toBe('DatabricksTinyIntBuilder');
    expect((DatabricksTinyInt as any)[entityKind]).toBe('DatabricksTinyInt');
  });
});

describe('float()', () => {
  it('creates a DatabricksFloatBuilder and builds a FLOAT column', () => {
    expect(float()).toBeInstanceOf(DatabricksFloatBuilder);
    const col = buildColumn(float());
    expect(col).toBeInstanceOf(DatabricksFloat);
    expect(col.getSQLType()).toBe('FLOAT');
  });

  it('has correct entityKind', () => {
    expect((DatabricksFloatBuilder as any)[entityKind]).toBe('DatabricksFloatBuilder');
    expect((DatabricksFloat as any)[entityKind]).toBe('DatabricksFloat');
  });
});

describe('double()', () => {
  it('creates a DatabricksDoubleBuilder and builds a DOUBLE column', () => {
    expect(double()).toBeInstanceOf(DatabricksDoubleBuilder);
    const col = buildColumn(double());
    expect(col).toBeInstanceOf(DatabricksDouble);
    expect(col.getSQLType()).toBe('DOUBLE');
  });

  it('has correct entityKind', () => {
    expect((DatabricksDoubleBuilder as any)[entityKind]).toBe('DatabricksDoubleBuilder');
    expect((DatabricksDouble as any)[entityKind]).toBe('DatabricksDouble');
  });
});

describe('decimal()', () => {
  it('creates a DatabricksDecimalBuilder', () => {
    expect(decimal({ precision: 18, scale: 6 })).toBeInstanceOf(DatabricksDecimalBuilder);
  });

  it('renders DECIMAL(precision, scale) with provided values', () => {
    const col = buildColumn(decimal({ precision: 18, scale: 6 }));
    expect(col).toBeInstanceOf(DatabricksDecimal);
    expect(col.getSQLType()).toBe('DECIMAL(18, 6)');
  });

  it('defaults to DECIMAL(10, 0) when no config is provided', () => {
    const col = buildColumn(decimal());
    expect(col.getSQLType()).toBe('DECIMAL(10, 0)');
  });

  it('accepts a name as the first argument', () => {
    const col = buildColumn(decimal('amount', { precision: 12, scale: 4 }), 'amount');
    expect(col.getSQLType()).toBe('DECIMAL(12, 4)');
  });

  it('has correct entityKind', () => {
    expect((DatabricksDecimalBuilder as any)[entityKind]).toBe('DatabricksDecimalBuilder');
    expect((DatabricksDecimal as any)[entityKind]).toBe('DatabricksDecimal');
  });
});

describe('boolean()', () => {
  it('creates a DatabricksBooleanBuilder and builds a BOOLEAN column', () => {
    expect(boolean()).toBeInstanceOf(DatabricksBooleanBuilder);
    const col = buildColumn(boolean());
    expect(col).toBeInstanceOf(DatabricksBoolean);
    expect(col.getSQLType()).toBe('BOOLEAN');
  });

  it('has correct entityKind', () => {
    expect((DatabricksBooleanBuilder as any)[entityKind]).toBe('DatabricksBooleanBuilder');
    expect((DatabricksBoolean as any)[entityKind]).toBe('DatabricksBoolean');
  });

  it('mapFromDriverValue handles booleans, numbers, and strings', () => {
    const col = buildColumn(boolean()) as DatabricksBoolean;
    expect(col.mapFromDriverValue(true)).toBe(true);
    expect(col.mapFromDriverValue(false)).toBe(false);
    expect(col.mapFromDriverValue(1)).toBe(true);
    expect(col.mapFromDriverValue(0)).toBe(false);
    expect(col.mapFromDriverValue('true')).toBe(true);
    expect(col.mapFromDriverValue('false')).toBe(false);
    expect(col.mapFromDriverValue('1')).toBe(true);
  });
});

describe('date()', () => {
  it('creates a DatabricksDateBuilder and builds a DATE column', () => {
    expect(date()).toBeInstanceOf(DatabricksDateBuilder);
    const col = buildColumn(date());
    expect(col).toBeInstanceOf(DatabricksDate);
    expect(col.getSQLType()).toBe('DATE');
  });

  it('has correct entityKind', () => {
    expect((DatabricksDateBuilder as any)[entityKind]).toBe('DatabricksDateBuilder');
    expect((DatabricksDate as any)[entityKind]).toBe('DatabricksDate');
  });
});

describe('timestamp()', () => {
  it('creates a DatabricksTimestampBuilder', () => {
    expect(timestamp()).toBeInstanceOf(DatabricksTimestampBuilder);
  });

  it('builds a TIMESTAMP column', () => {
    const col = buildColumn(timestamp());
    expect(col).toBeInstanceOf(DatabricksTimestamp);
    expect(col.getSQLType()).toBe('TIMESTAMP');
  });

  it('has correct entityKind', () => {
    expect((DatabricksTimestampBuilder as any)[entityKind]).toBe('DatabricksTimestampBuilder');
    expect((DatabricksTimestamp as any)[entityKind]).toBe('DatabricksTimestamp');
  });

  it('mapFromDriverValue returns a Date for strings, numbers, and Date instances', () => {
    const col = buildColumn(timestamp()) as DatabricksTimestamp;
    const fromString = col.mapFromDriverValue('2024-01-02T03:04:05.000Z');
    expect(fromString).toBeInstanceOf(Date);
    expect((fromString as Date).toISOString()).toBe('2024-01-02T03:04:05.000Z');

    const fromNumber = col.mapFromDriverValue(0);
    expect(fromNumber).toBeInstanceOf(Date);
    expect((fromNumber as Date).getTime()).toBe(0);

    const original = new Date('2024-06-01T00:00:00.000Z');
    expect(col.mapFromDriverValue(original)).toBe(original);
  });
});

describe('timestampNtz()', () => {
  it('builds a TIMESTAMP_NTZ column', () => {
    const col = buildColumn(timestampNtz());
    expect(col).toBeInstanceOf(DatabricksTimestamp);
    expect(col.getSQLType()).toBe('TIMESTAMP_NTZ');
  });
});

describe('binary()', () => {
  it('creates a DatabricksBinaryBuilder and builds a BINARY column', () => {
    expect(binary()).toBeInstanceOf(DatabricksBinaryBuilder);
    const col = buildColumn(binary());
    expect(col).toBeInstanceOf(DatabricksBinary);
    expect(col.getSQLType()).toBe('BINARY');
  });

  it('has correct entityKind', () => {
    expect((DatabricksBinaryBuilder as any)[entityKind]).toBe('DatabricksBinaryBuilder');
    expect((DatabricksBinary as any)[entityKind]).toBe('DatabricksBinary');
  });
});

describe('variant()', () => {
  it('creates a DatabricksVariantBuilder and builds a VARIANT column', () => {
    expect(variant()).toBeInstanceOf(DatabricksVariantBuilder);
    const col = buildColumn(variant());
    expect(col).toBeInstanceOf(DatabricksVariant);
    expect(col.getSQLType()).toBe('VARIANT');
  });

  it('has correct entityKind', () => {
    expect((DatabricksVariantBuilder as any)[entityKind]).toBe('DatabricksVariantBuilder');
    expect((DatabricksVariant as any)[entityKind]).toBe('DatabricksVariant');
  });

  it('mapFromDriverValue JSON.parses string values', () => {
    const col = buildColumn(variant()) as DatabricksVariant;
    expect(col.mapFromDriverValue('{"a":1}')).toEqual({ a: 1 });
    expect(col.mapFromDriverValue('[1,2,3]')).toEqual([1, 2, 3]);
  });

  it('mapFromDriverValue passes through non-string values unchanged', () => {
    const col = buildColumn(variant()) as DatabricksVariant;
    const obj = { a: 1 };
    expect(col.mapFromDriverValue(obj)).toBe(obj);
    expect(col.mapFromDriverValue(42)).toBe(42);
  });

  it('mapFromDriverValue returns the original string when JSON.parse fails', () => {
    const col = buildColumn(variant()) as DatabricksVariant;
    expect(col.mapFromDriverValue('not json')).toBe('not json');
  });

  it('mapToDriverValue JSON.stringifies non-string values', () => {
    const col = buildColumn(variant()) as DatabricksVariant;
    expect(col.mapToDriverValue({ a: 1 })).toBe('{"a":1}');
    expect(col.mapToDriverValue([1, 2])).toBe('[1,2]');
    expect(col.mapToDriverValue(123)).toBe('123');
  });

  it('mapToDriverValue passes string values through unchanged', () => {
    const col = buildColumn(variant()) as DatabricksVariant;
    expect(col.mapToDriverValue('already-a-string')).toBe('already-a-string');
  });
});

describe('column entityKind hierarchy', () => {
  it('all builders are DatabricksColumnBuilder instances', () => {
    expect(is(string(), DatabricksColumnBuilder)).toBe(true);
    expect(is(int(), DatabricksColumnBuilder)).toBe(true);
    expect(is(bigint(), DatabricksColumnBuilder)).toBe(true);
    expect(is(decimal(), DatabricksColumnBuilder)).toBe(true);
    expect(is(boolean(), DatabricksColumnBuilder)).toBe(true);
    expect(is(timestamp(), DatabricksColumnBuilder)).toBe(true);
    expect(is(variant(), DatabricksColumnBuilder)).toBe(true);
  });
});
