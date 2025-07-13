import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach
} from 'vitest'
import { isLogicList, logicListToArray, lvar, resetVarCounter } from '../core/kernel.ts';
import { eq, and, or } from '../core/combinators.ts';
import { collecto } from "../relations/aggregates-subqueries.ts";
import { not } from '../relations/control.ts';
import { query } from '../query.ts';
import { makeRelDB } from './index.ts';

describe('SQL Backend Integration Tests', () => {
  let db: Awaited<ReturnType<typeof makeRelDB>>;

  beforeEach(async () => {
    resetVarCounter();
    db = await makeRelDB({
      client: 'better-sqlite3',
      connection: {
        filename: ':memory:'
      },
      useNullAsDefault: true
    });

    // Create and populate test table
    await db.db.schema.createTable('people', table => {
      table.integer('id').primary();
      table.string('name');
      table.integer('age');
    });

    await db.db.schema.createTable('parents', table => {
      table.string('parent');
      table.string('child');
    });

    await db.db('people').insert([
      {
        id: 1,
        name: 'alice',
        age: 30 
      },
      {
        id: 2,
        name: 'bob',
        age: 25 
      },
      {
        id: 3,
        name: 'charlie',
        age: 35 
      },
      {
        id: 4,
        name: 'diana',
        age: 28 
      }
    ]);

    await db.db('parents').insert([
      {
        parent: 'alice',
        child: 'bob' 
      },
      {
        parent: 'bob',
        child: 'charlie' 
      },
      {
        parent: 'bob',
        child: 'diana' 
      }
    ]);
  });

  afterEach(async () => {
    await db.db.destroy();
  });

  describe('SQL backend with logic combinators', () => {
    it('should use or logic with database relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const goal = and(
        db.rel('people')({
          name,
          age 
        }),
        or(eq(age, 25), eq(age, 30))
      );
      const results = await query()
        .select($ => ({
          name,
          age
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(2);
      // @ts-expect-error
      const sortedResults = results.sort((a, b) => a.age - b.age);
      expect(sortedResults).toEqual([
        {
          name: 'bob',
          age: 25 
        },
        {
          name: 'alice',
          age: 30 
        }
      ]);
    });

    it('should find grandparents using and logic', async () => {
      const grandparent = lvar('grandparent');
      const grandchild = lvar('grandchild');
      const parent = lvar('parent');
      
      const goal = and(
        db.rel('parents')({
          parent: grandparent,
          child: parent 
        }),
        db.rel('parents')({
          parent,
          child: grandchild 
        })
      );
      const results = await query()
        .select($ => ({
          grandparent,
          grandchild
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(2);
      // @ts-expect-error
      const sortedResults = results.sort((a, b) => a.grandchild.localeCompare(b.grandchild));
      expect(sortedResults).toEqual([
        {
          grandparent: 'alice',
          grandchild: 'charlie' 
        },
        {
          grandparent: 'alice',
          grandchild: 'diana' 
        }
      ]);
    });

    it('should use not with database relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const goal = and(
        db.rel('people')({
          name,
          age 
        }),
        not(eq(name, 'alice'))
      );
      const results = await query()
        .select($ => ({
          name,
          age
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(3); // Everyone except alice
      const names = results.map(r => r.name).sort();
      expect(names).toEqual(['bob', 'charlie', 'diana']);
    });

    it('should use collecto with database relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const names = lvar('names');
      
      const goal = collecto(
        name,
        and(
          db.rel('people')({
            name,
            age 
          }),
          or(eq(age, 25), eq(age, 30)) // Only bob and alice
        ),
        names
      );
      const results = await query()
        .select($ => ({
          names
        }))
        .where($ => goal)
        .toArray();
      expect(results).toHaveLength(1);
      const arr = results[0].names;
      if (!Array.isArray(arr)) throw new Error('Expected array');
      expect(arr.sort()).toEqual(['alice', 'bob']);
    });
  });
});