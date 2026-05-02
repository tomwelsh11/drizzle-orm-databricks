import type { Logger } from "drizzle-orm/logger";
import { NoopLogger } from "drizzle-orm/logger";

export const DEFAULT_POOL_MAX_SIZE = 10;
export const DEFAULT_POOL_ACQUIRE_TIMEOUT_MS = 30_000;

export class PoolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PoolError";
  }
}

export interface PoolHooks<TItem> {
  create(): Promise<TItem>;
  destroy(item: TItem): Promise<void>;
  validate(item: TItem): Promise<boolean> | boolean;
}

export interface PoolOptions<TItem> extends PoolHooks<TItem> {
  maxSize?: number;
  acquireTimeoutMs?: number;
  logger?: Logger;
}

interface Waiter<TItem> {
  resolve: (item: TItem) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
}

/**
 * Generic resource pool. Items are created lazily up to `maxSize`. Acquire
 * requests beyond capacity wait in a FIFO queue until something is released
 * or the configured timeout elapses.
 */
export class Pool<TItem> {
  private readonly hooks: PoolHooks<TItem>;
  private readonly maxSize: number;
  private readonly acquireTimeoutMs: number;
  private readonly logger: Logger;

  private readonly availableItems: TItem[] = [];
  private readonly inUseItems = new Set<TItem>();
  private waiters: Waiter<TItem>[] = [];
  private draining = false;

  constructor(options: PoolOptions<TItem>) {
    this.hooks = {
      create: options.create,
      destroy: options.destroy,
      validate: options.validate,
    };
    this.maxSize = options.maxSize ?? DEFAULT_POOL_MAX_SIZE;
    this.acquireTimeoutMs = options.acquireTimeoutMs ?? DEFAULT_POOL_ACQUIRE_TIMEOUT_MS;
    this.logger = options.logger ?? new NoopLogger();

    if (this.maxSize < 1) {
      throw new PoolError(`Pool maxSize must be >= 1, got ${this.maxSize}`);
    }
  }

  get size(): number {
    return this.availableItems.length + this.inUseItems.size;
  }

  get available(): number {
    return this.availableItems.length;
  }

  get inUse(): number {
    return this.inUseItems.size;
  }

  get pending(): number {
    return this.waiters.length;
  }

  async acquire(): Promise<TItem> {
    if (this.draining) {
      throw new PoolError("Cannot acquire from a drained pool");
    }

    while (this.availableItems.length > 0) {
      const candidate = this.availableItems.pop()!;
      if (await this.hooks.validate(candidate)) {
        this.inUseItems.add(candidate);
        return candidate;
      }
      await this.safeDestroy(candidate);
    }

    if (this.size < this.maxSize) {
      const item = await this.hooks.create();
      this.inUseItems.add(item);
      return item;
    }

    return this.waitForItem();
  }

  async release(item: TItem): Promise<void> {
    if (!this.inUseItems.delete(item)) {
      this.logger.logQuery("[pool] release called for unknown item", []);
      return;
    }

    if (this.draining) {
      await this.safeDestroy(item);
      return;
    }

    if (!(await this.hooks.validate(item))) {
      await this.safeDestroy(item);
      await this.refillWaiter();
      return;
    }

    const waiter = this.waiters.shift();
    if (waiter) {
      clearTimeout(waiter.timeoutId);
      this.inUseItems.add(item);
      waiter.resolve(item);
      return;
    }

    this.availableItems.push(item);
  }

  async drain(): Promise<void> {
    this.draining = true;

    const waiters = this.waiters;
    this.waiters = [];
    for (const waiter of waiters) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new PoolError("Pool is draining"));
    }

    const toDestroy = [...this.availableItems, ...this.inUseItems];
    this.availableItems.length = 0;
    this.inUseItems.clear();
    for (const item of toDestroy) {
      await this.safeDestroy(item);
    }
  }

  private async refillWaiter(): Promise<void> {
    while (this.waiters.length > 0 && this.size < this.maxSize) {
      const waiter = this.waiters.shift()!;
      clearTimeout(waiter.timeoutId);
      try {
        const item = await this.hooks.create();
        this.inUseItems.add(item);
        waiter.resolve(item);
        return;
      } catch (err) {
        waiter.reject(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }

  private waitForItem(): Promise<TItem> {
    return new Promise<TItem>((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const idx = this.waiters.findIndex((w) => w.timeoutId === timeoutId);
        if (idx !== -1) {
          this.waiters.splice(idx, 1);
          reject(new PoolError(`Pool acquire timed out after ${this.acquireTimeoutMs}ms`));
        }
      }, this.acquireTimeoutMs);

      this.waiters.push({ resolve, reject, timeoutId });
    });
  }

  private async safeDestroy(item: TItem): Promise<void> {
    try {
      await this.hooks.destroy(item);
    } catch {
      // best-effort
    }
  }
}
