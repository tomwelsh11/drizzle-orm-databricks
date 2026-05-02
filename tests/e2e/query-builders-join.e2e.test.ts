import { sql, eq, and, gt, lt, desc, asc } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { databricksTable, string, int } from '../../src';
import { closeDb, dropTable, getDb, hasCredentials } from './helpers';

const users = databricksTable('qb_join_users', {
  id: string('id'),
  name: string('name'),
  age: int('age'),
});

const posts = databricksTable('qb_join_posts', {
  id: string('id'),
  userId: string('user_id'),
  title: string('title'),
  likes: int('likes'),
});

const bt = (n: string) => '`' + n + '`';

describe.skipIf(!hasCredentials())('Query builder JOINs (e2e)', () => {
  beforeAll(async () => {
    const db = getDb();
    await dropTable(db, 'qb_join_users');
    await dropTable(db, 'qb_join_posts');

    await db.execute(sql.raw(
      `CREATE TABLE IF NOT EXISTS ${bt('qb_join_users')} (
        id STRING, name STRING, age INT
      ) USING DELTA`,
    ));
    await db.execute(sql.raw(
      `CREATE TABLE IF NOT EXISTS ${bt('qb_join_posts')} (
        id STRING, user_id STRING, title STRING, likes INT
      ) USING DELTA`,
    ));

    // 4 users — u4 has no posts
    await db.execute(sql.raw(
      `INSERT INTO ${bt('qb_join_users')} (id, name, age) VALUES
        ('u1', 'Alice', 30),
        ('u2', 'Bob', 25),
        ('u3', 'Carol', 40),
        ('u4', 'Dan', 22)`,
    ));

    // 5 posts: u1 has 2, u2 has 1, u3 has 1, plus one orphan with user_id 'u_ghost'
    await db.execute(sql.raw(
      `INSERT INTO ${bt('qb_join_posts')} (id, user_id, title, likes) VALUES
        ('p1', 'u1', 'Hello World', 50),
        ('p2', 'u1', 'Second Post', 5),
        ('p3', 'u2', 'Bobs Thoughts', 20),
        ('p4', 'u3', 'Carols Recipes', 100),
        ('p5', 'u_ghost', 'Orphan Post', 1)`,
    ));
  });

  afterAll(async () => {
    const db = getDb();
    try {
      await dropTable(db, 'qb_join_users');
      await dropTable(db, 'qb_join_posts');
    } finally {
      await closeDb();
    }
  });

  it('INNER JOIN returns only matching rows', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId));

    // 4 matching: u1×p1, u1×p2, u2×p3, u3×p4 (u4 and p5 excluded)
    expect(rows).toHaveLength(4);
    for (const row of rows) {
      expect(row.qb_join_users).not.toBeNull();
      expect(row.qb_join_posts).not.toBeNull();
      expect(row.qb_join_users.id).toBe(row.qb_join_posts.userId);
    }
  });

  it('INNER JOIN with partial select returns flat shape', async () => {
    const db = getDb();
    const rows = await db.select({
      userName: users.name,
      postTitle: posts.title,
    })
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId));

    expect(rows).toHaveLength(4);
    for (const row of rows) {
      expect(typeof row.userName).toBe('string');
      expect(typeof row.postTitle).toBe('string');
      // No nested table grouping in partial select
      expect((row as Record<string, unknown>).qb_join_users).toBeUndefined();
      expect((row as Record<string, unknown>).qb_join_posts).toBeUndefined();
    }
  });

  it('LEFT JOIN returns null on right side for users without posts', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .leftJoin(posts, eq(users.id, posts.userId));

    // u1 (×2), u2, u3, u4 (with null) = 5 rows
    expect(rows).toHaveLength(5);

    const danRows = rows.filter((r) => r.qb_join_users.id === 'u4');
    expect(danRows).toHaveLength(1);
    expect(danRows[0]!.qb_join_posts).toBeNull();

    const aliceRows = rows.filter((r) => r.qb_join_users.id === 'u1');
    expect(aliceRows).toHaveLength(2);
    for (const row of aliceRows) {
      expect(row.qb_join_posts).not.toBeNull();
    }
  });

  it('LEFT JOIN includes every left-side row', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .leftJoin(posts, eq(users.id, posts.userId));

    const distinctUserIds = new Set(rows.map((r) => r.qb_join_users.id));
    expect(distinctUserIds.size).toBe(4);
    expect(distinctUserIds).toEqual(new Set(['u1', 'u2', 'u3', 'u4']));
  });

  it('RIGHT JOIN includes posts without matching users', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .rightJoin(posts, eq(users.id, posts.userId));

    // All 5 posts appear; the orphan p5 has null users
    expect(rows).toHaveLength(5);

    const orphan = rows.find((r) => r.qb_join_posts?.id === 'p5');
    expect(orphan).toBeDefined();
    expect(orphan!.qb_join_users).toBeNull();

    const matched = rows.filter((r) => r.qb_join_users !== null);
    expect(matched).toHaveLength(4);
  });

  it('FULL JOIN returns all rows from both sides', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .fullJoin(posts, eq(users.id, posts.userId));

    // 4 matched + u4 (no posts) + p5 (no user) = 6 rows
    expect(rows).toHaveLength(6);

    const userOnly = rows.find((r) => r.qb_join_users?.id === 'u4');
    expect(userOnly).toBeDefined();
    expect(userOnly!.qb_join_posts).toBeNull();

    const postOnly = rows.find((r) => r.qb_join_posts?.id === 'p5');
    expect(postOnly).toBeDefined();
    expect(postOnly!.qb_join_users).toBeNull();
  });

  it('JOIN with WHERE on left table filters left-side rows', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .where(gt(users.age, 28));

    // Only Alice (30) and Carol (40) qualify; Alice has 2 posts, Carol has 1 = 3 rows
    expect(rows).toHaveLength(3);
    for (const row of rows) {
      expect(row.qb_join_users.age).toBeGreaterThan(28);
    }
  });

  it('JOIN with WHERE on right table filters right-side rows', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .where(gt(posts.likes, 10));

    // p1 (50), p3 (20), p4 (100) — 3 posts above threshold and matching a user
    expect(rows).toHaveLength(3);
    for (const row of rows) {
      expect(row.qb_join_posts.likes).toBeGreaterThan(10);
    }
  });

  it('JOIN with WHERE combining both tables', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .where(and(gt(users.age, 24), gt(posts.likes, 10)));

    // age > 24 → u1, u2, u3; combined with likes > 10 → p1, p3, p4
    expect(rows).toHaveLength(3);
    for (const row of rows) {
      expect(row.qb_join_users.age).toBeGreaterThan(24);
      expect(row.qb_join_posts.likes).toBeGreaterThan(10);
    }
  });

  it('JOIN with ORDER BY orders the result set', async () => {
    const db = getDb();
    const rows = await db.select({
      userName: users.name,
      likes: posts.likes,
    })
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .orderBy(desc(posts.likes));

    expect(rows).toHaveLength(4);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i - 1]!.likes!).toBeGreaterThanOrEqual(rows[i]!.likes!);
    }
    expect(rows[0]!.likes).toBe(100);
  });

  it('JOIN with LIMIT bounds the result set', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .orderBy(asc(posts.id))
      .limit(2);

    expect(rows).toHaveLength(2);
  });

  it('JOIN with ORDER BY and LIMIT picks the top N', async () => {
    const db = getDb();
    const rows = await db.select({
      title: posts.title,
      likes: posts.likes,
    })
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .orderBy(desc(posts.likes))
      .limit(2);

    expect(rows).toHaveLength(2);
    expect(rows[0]!.likes).toBe(100);
    expect(rows[1]!.likes).toBe(50);
  });

  it('JOIN with impossible WHERE returns empty set', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .innerJoin(posts, eq(users.id, posts.userId))
      .where(and(gt(posts.likes, 1000), lt(posts.likes, 0)));

    expect(rows).toEqual([]);
  });

  it('LEFT JOIN combined with WHERE filtering on left side', async () => {
    const db = getDb();
    const rows = await db.select()
      .from(users)
      .leftJoin(posts, eq(users.id, posts.userId))
      .where(eq(users.id, 'u4'));

    // Dan has no posts; LEFT JOIN keeps him with null posts
    expect(rows).toHaveLength(1);
    expect(rows[0]!.qb_join_users.id).toBe('u4');
    expect(rows[0]!.qb_join_posts).toBeNull();
  });
});
