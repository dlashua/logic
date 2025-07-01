import { makeRelDBWithMerger } from "../facts-sql/facts-sql-with-merger.ts";
import { lvar } from "../core/kernel.ts";
import { and, eq } from "../core/combinators.ts";
import { createLogicVarProxy, query } from "../query.ts";

// Test data setup
const testDB = {
  client: 'better-sqlite3',
  connection: {
    filename: ':memory:'
  },
  useNullAsDefault: true
};

const { rel, getQueries, clearQueries, db } = await makeRelDBWithMerger(testDB, {}, {}, 50);


async function setupTestData() {  
  // Create test tables
  await db.schema.createTable('users', table => {
    table.increments('id');
    table.string('name');
    table.integer('age');
    table.string('department');
    table.integer('manager_id');
  });
  
  await db.schema.createTable('projects', table => {
    table.increments('id');
    table.string('name');
    table.integer('user_id');
    table.string('status');
  });
  
  // Insert test data
  await db('users').insert([
    {
      id: 1,
      name: 'Alice',
      age: 30,
      department: 'Engineering',
      manager_id: 0,
    },
    {
      id: 2,
      name: 'Bob',
      age: 25,
      department: 'Engineering',
      manager_id: 1,
    },
    {
      id: 3,
      name: 'Charlie',
      age: 35,
      department: 'Sales',
      manager_id: 1,
    },
    {
      id: 4,
      name: 'Daren',
      age: 35,
      department: 'Sales',
      manager_id: 3,
    },
    {
      id: 5,
      name: 'Elena',
      age: 35,
      department: 'Sales', 
      manager_id: 3,
    },
  ]);
  
  await db('projects').insert([
    {
      id: 1,
      name: 'Project A',
      user_id: 1,
      status: 'active' 
    },
    {
      id: 2,
      name: 'Project B',
      user_id: 4,
      status: 'completed' 
    },
    {
      id: 3,
      name: 'Project C',
      user_id: 5,
      status: 'active' 
    },
  ]);
  
  return db;
}

async function testQueryMerging() {
  console.log('Testing query merging...');
      
  const q_users = rel("users");
  const q_projects = rel("projects");
  
  console.log('Queries created, starting execution...');
  clearQueries();
  
  const results = await query()
    .select("*")
    .where($ => [
      // eq("Elena", $.user_name),
      q_users({
        id: $.manager_id,
        name: $.manager_name,
        department: $.manager_department, 
      }),
      q_users({
        manager_id: $.manager_id,
        id: $.user_id,
        name: $.user_name,
      }),
      // q_projects({
      //   user_id: $.user_id,
      //   name: $.project_name,
      //   status: $.project_status, 
      // })
    ])
    .toArray()
  ;
  
  console.log('Results:', results);
  console.log('Total SQL queries executed:', getQueries().length);
  console.log('SQL queries:', getQueries());
  
  // Expected: fewer queries than if run separately due to merging
  if (getQueries().length < 4) {
    console.log('✓ Query merging appears to be working - fewer queries than expected');
  } else {
    console.log('✗ Query merging may not be working - more queries than expected');
  }
  
  return results;
}

async function testIndependentQueries() {
  console.log('\nTesting independent queries (should not merge)...');
  
    
  // Create variables that don't share any IDs
  const userId1 = lvar('user_id_1');
  const userId2 = lvar('user_id_2');
  const userName1 = lvar('user_name_1');
  const userName2 = lvar('user_name_2');
  
  // Create queries with completely different variables
  const query1 = rel('users')({
    id: userId1,
    name: userName1 
  });
  const query2 = rel('users')({
    id: userId2,
    name: userName2 
  });
  
  clearQueries();
  
  const combinedGoal = and(query1, query2);
  const results = [];
  
  const initialSubst = new Map();
  const substStream = combinedGoal(initialSubst);
  
  for await (const subst of substStream) {
    if (subst) {
      results.push({
        userId1: subst.get(userId1.id),
        userName1: subst.get(userName1.id),
        userId2: subst.get(userId2.id),
        userName2: subst.get(userName2.id)
      });
    }
  }
  
  console.log('Results:', results);
  console.log('Total SQL queries executed:', getQueries().length);
  console.log('SQL queries:', getQueries());
  
  // Expected: same number of queries as relations since no merging should occur
  console.log('Independent queries completed');
  
  return results;
}

async function runTests() {
  try {
    await setupTestData();
    await testQueryMerging();
    console.log('\n✓ All tests completed');
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runTests();
  process.exit();
}