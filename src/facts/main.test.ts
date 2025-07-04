import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach
} from 'vitest'
import { isLogicList, logicListToArray, lvar, resetVarCounter } from '../core/kernel.ts';
import { eq, and, or } from '../core/combinators.ts';
import { collecto } from '../relations/aggregates.ts';
import { not } from '../relations/control.ts';
import { query } from '../query.ts';
import { makeFactsObj, makeFacts } from './facts-memory.ts';

describe('MEM Backend Integration Tests', () => {
  let people: any;
  let parents: any;

  beforeEach(async () => {
    resetVarCounter();
    people = makeFactsObj(["id", "name", "age"]);
    parents = makeFactsObj(["parent", "child"]);

    const peopleData = [
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
    ];
    peopleData.forEach(x => people.set(x));

    const parentsData = [
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
    ];
    parentsData.forEach(x => parents.set(x));
  });

  describe('MEM backend with logic combinators', () => {
    it('should use or logic with mem relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const goal = and(
        people({
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
        parents({
          parent: grandparent,
          child: parent 
        }),
        parents({
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

    it('should use not with mem relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const goal = and(
        people({
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

    it('should use collecto with mem relations', async () => {
      const name = lvar('name');
      const age = lvar('age');
      const names = lvar('names');
      
      const goal = collecto(
        name,
        and(
          people({
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