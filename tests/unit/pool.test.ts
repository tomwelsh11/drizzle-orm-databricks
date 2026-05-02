import { describe, expect, it, vi } from "vitest";

import { Pool, PoolError } from "../../src/pool";

interface TestItem {
  id: number;
  destroyed: boolean;
  valid: boolean;
}

function makeFactory(
  overrides: { validate?: (item: TestItem) => boolean | Promise<boolean> } = {},
) {
  let nextId = 0;
  const created: TestItem[] = [];
  const destroyed: TestItem[] = [];

  const create = vi.fn(async (): Promise<TestItem> => {
    const item: TestItem = { id: nextId++, destroyed: false, valid: true };
    created.push(item);
    return item;
  });

  const destroy = vi.fn(async (item: TestItem) => {
    item.destroyed = true;
    destroyed.push(item);
  });

  const validate = vi.fn(async (item: TestItem) => {
    if (overrides.validate) return overrides.validate(item);
    return item.valid;
  });

  return { create, destroy, validate, created, destroyed };
}

describe("Pool", () => {
  it("creates an item lazily on first acquire", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });

    expect(f.create).not.toHaveBeenCalled();
    expect(pool.size).toBe(0);

    const item = await pool.acquire();

    expect(f.create).toHaveBeenCalledTimes(1);
    expect(pool.size).toBe(1);
    expect(pool.inUse).toBe(1);
    expect(item.id).toBe(0);
  });

  it("reuses released items instead of creating new ones", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });

    const a = await pool.acquire();
    await pool.release(a);

    expect(pool.available).toBe(1);
    expect(pool.inUse).toBe(0);

    const b = await pool.acquire();
    expect(b).toBe(a);
    expect(f.create).toHaveBeenCalledTimes(1);
  });

  it("respects maxSize and queues acquires when at capacity", async () => {
    const f = makeFactory();
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 2,
    });

    const a = await pool.acquire();
    const b = await pool.acquire();
    expect(pool.size).toBe(2);

    let cResolved: TestItem | undefined;
    const cPromise = pool.acquire().then((item) => {
      cResolved = item;
    });

    await Promise.resolve();
    expect(cResolved).toBeUndefined();
    expect(pool.pending).toBe(1);

    await pool.release(a);
    await cPromise;
    expect(cResolved).toBe(a);
    expect(pool.pending).toBe(0);

    await pool.release(b);
    await pool.release(a);
  });

  it("destroys invalid items and creates new ones on acquire", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });

    const a = await pool.acquire();
    a.valid = false;
    await pool.release(a);

    // Releasing an invalid item destroys it.
    expect(a.destroyed).toBe(true);
    expect(pool.available).toBe(0);
    expect(pool.size).toBe(0);

    const b = await pool.acquire();
    expect(b).not.toBe(a);
    expect(f.create).toHaveBeenCalledTimes(2);
  });

  it("destroys invalid items found in availableItems on acquire", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });

    const a = await pool.acquire();
    await pool.release(a);
    expect(pool.available).toBe(1);

    // Now turn it invalid AFTER release. Acquire should detect, destroy, recreate.
    a.valid = false;

    const b = await pool.acquire();
    expect(a.destroyed).toBe(true);
    expect(b).not.toBe(a);
    expect(f.create).toHaveBeenCalledTimes(2);
  });

  it("times out queued acquire requests", async () => {
    vi.useFakeTimers();
    try {
      const f = makeFactory();
      const pool = new Pool({
        create: f.create,
        destroy: f.destroy,
        validate: f.validate,
        maxSize: 1,
        acquireTimeoutMs: 50,
      });

      await pool.acquire();
      const promise = pool.acquire();

      const expectation = expect(promise).rejects.toBeInstanceOf(PoolError);
      await vi.advanceTimersByTimeAsync(60);
      await expectation;
    } finally {
      vi.useRealTimers();
    }
  });

  it("hands off a released item to the first waiter (FIFO)", async () => {
    const f = makeFactory();
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 1,
    });

    const a = await pool.acquire();
    const order: string[] = [];

    const w1 = pool.acquire().then((it) => {
      order.push(`w1:${it.id}`);
      return it;
    });
    const w2 = pool.acquire().then((it) => {
      order.push(`w2:${it.id}`);
      return it;
    });

    await Promise.resolve();
    expect(pool.pending).toBe(2);

    await pool.release(a);
    const got1 = await w1;
    expect(got1).toBe(a);
    expect(pool.pending).toBe(1);

    await pool.release(got1);
    const got2 = await w2;
    expect(got2).toBe(a);
    expect(order).toEqual(["w1:0", "w2:0"]);
  });

  it("released invalid items create a fresh item for waiters when room exists", async () => {
    const f = makeFactory();
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 1,
    });

    const a = await pool.acquire();
    const waitPromise = pool.acquire();

    await Promise.resolve();
    expect(pool.pending).toBe(1);

    a.valid = false;
    await pool.release(a);
    expect(a.destroyed).toBe(true);

    const b = await waitPromise;
    expect(b).not.toBe(a);
    expect(f.create).toHaveBeenCalledTimes(2);
  });

  it("drains the pool: destroys items and rejects waiters", async () => {
    const f = makeFactory();
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 2,
    });

    const a = await pool.acquire();
    const b = await pool.acquire();
    expect(pool.size).toBe(2);

    const waiter = pool.acquire();
    await Promise.resolve();
    expect(pool.pending).toBe(1);

    await pool.drain();

    await expect(waiter).rejects.toBeInstanceOf(PoolError);
    expect(a.destroyed).toBe(true);
    expect(b.destroyed).toBe(true);
    expect(pool.size).toBe(0);
  });

  it("rejects acquire after drain", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });
    await pool.drain();
    await expect(pool.acquire()).rejects.toBeInstanceOf(PoolError);
  });

  it("releasing an item already destroyed during drain is safe", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });
    const a = await pool.acquire();
    await pool.drain();
    // After drain, the item was destroyed. Releasing it again must not throw.
    await pool.release(a);
    expect(pool.size).toBe(0);
  });

  it("ignores release of unknown items", async () => {
    const f = makeFactory();
    const pool = new Pool({ create: f.create, destroy: f.destroy, validate: f.validate });
    const stranger: TestItem = { id: 999, destroyed: false, valid: true };
    await pool.release(stranger);
    expect(stranger.destroyed).toBe(false);
    expect(pool.size).toBe(0);
  });

  it("rejects maxSize < 1", () => {
    const f = makeFactory();
    expect(
      () => new Pool({ create: f.create, destroy: f.destroy, validate: f.validate, maxSize: 0 }),
    ).toThrow(PoolError);
  });

  it("does not exceed maxSize under concurrent acquire with slow validate", async () => {
    const f = makeFactory({
      validate: async (item) => {
        await new Promise((r) => setTimeout(r, 10));
        return item.valid;
      },
    });
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 1,
    });

    const a = await pool.acquire();
    await pool.release(a);
    expect(pool.available).toBe(1);

    // Fire two concurrent acquires. With the fix, the second sees size=1
    // during the first's slow validate and queues as a waiter instead of
    // creating a new item that would exceed maxSize.
    const p1 = pool.acquire();
    const p2 = pool.acquire();

    const r1 = await p1;
    expect(pool.pending).toBe(1);
    expect(f.create).toHaveBeenCalledTimes(1);

    await pool.release(r1);
    const r2 = await p2;
    expect(r2).toBe(r1);
    expect(f.create).toHaveBeenCalledTimes(1);
    expect(pool.size).toBe(1);

    await pool.release(r2);
  });

  it("does not exceed maxSize when releasing valid items (release bug fix)", async () => {
    // The reference implementation destroys valid items at release time when
    // size == maxSize, even though they're already counted in inUseItems.
    // Our pool returns them to availableItems instead.
    const f = makeFactory();
    const pool = new Pool({
      create: f.create,
      destroy: f.destroy,
      validate: f.validate,
      maxSize: 2,
    });

    const a = await pool.acquire();
    const b = await pool.acquire();
    expect(pool.size).toBe(2);

    await pool.release(a);
    expect(a.destroyed).toBe(false);
    expect(pool.available).toBe(1);
    expect(pool.size).toBe(2);

    await pool.release(b);
    expect(b.destroyed).toBe(false);
    expect(pool.available).toBe(2);
    expect(pool.size).toBe(2);
    expect(f.create).toHaveBeenCalledTimes(2);
  });
});
