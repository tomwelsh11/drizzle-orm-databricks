import { describe, expect, it } from 'vitest';
import {
  DatabricksConnectionError,
  DatabricksUnsupportedError,
} from '../../src/errors.js';

describe('DatabricksUnsupportedError', () => {
  it('formats a message with the feature name', () => {
    const err = new DatabricksUnsupportedError('.returning()');
    expect(err.message).toBe(
      '.returning() is not supported by the Databricks adapter for Drizzle ORM.',
    );
    expect(err.name).toBe('DatabricksUnsupportedError');
  });

  it('appends the alternative when provided', () => {
    const err = new DatabricksUnsupportedError(
      '.returning()',
      'Query the row by primary key after insert.',
    );
    expect(err.message).toBe(
      '.returning() is not supported by the Databricks adapter for Drizzle ORM. Query the row by primary key after insert.',
    );
  });

  it('is an instance of Error', () => {
    expect(new DatabricksUnsupportedError('x')).toBeInstanceOf(Error);
  });
});

describe('DatabricksConnectionError', () => {
  it('captures the message and cause', () => {
    const cause = new Error('socket reset');
    const err = new DatabricksConnectionError('Failed to connect', cause);
    expect(err.message).toBe('Failed to connect');
    expect(err.name).toBe('DatabricksConnectionError');
    expect(err.cause).toBe(cause);
  });

  it('allows omitting cause', () => {
    const err = new DatabricksConnectionError('Failed to connect');
    expect(err.cause).toBeUndefined();
  });

  it('is an instance of Error', () => {
    expect(new DatabricksConnectionError('x')).toBeInstanceOf(Error);
  });
});
