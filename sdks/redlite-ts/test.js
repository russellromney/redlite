const { RedliteDb } = require('./index.js');

console.log('Testing RedliteDb native module...\n');

// Create in-memory database
const db = RedliteDb.openMemory();
console.log('Created in-memory database');

// Test SET/GET
db.set('hello', Buffer.from('world'));
const value = db.get('hello');
console.log('GET hello:', value?.toString());
console.assert(value?.toString() === 'world', 'GET should return "world"');

// Test INCR
db.set('counter', Buffer.from('0'));
const n1 = db.incr('counter');
const n2 = db.incr('counter');
console.log('INCR counter:', n1, n2);
console.assert(n1 === 1 && n2 === 2, 'INCR should work');

// Test KEYS
db.set('user:1', Buffer.from('alice'));
db.set('user:2', Buffer.from('bob'));
const keys = db.keys('user:*');
console.log('KEYS user:*:', keys);
console.assert(keys.length === 2, 'Should have 2 user keys');

// Test HSET/HGET
db.hset('myhash', 'field1', Buffer.from('value1'));
const hval = db.hget('myhash', 'field1');
console.log('HGET myhash field1:', hval?.toString());
console.assert(hval?.toString() === 'value1', 'HGET should work');

// Test LPUSH/LRANGE
db.lpush('mylist', [Buffer.from('a'), Buffer.from('b'), Buffer.from('c')]);
const listItems = db.lrange('mylist', 0, -1);
console.log('LRANGE mylist:', listItems.map(b => b.toString()));
console.assert(listItems.length === 3, 'List should have 3 items');

// Test SADD/SMEMBERS
db.sadd('myset', [Buffer.from('x'), Buffer.from('y'), Buffer.from('z')]);
const setMembers = db.smembers('myset');
console.log('SMEMBERS myset:', setMembers.map(b => b.toString()));
console.assert(setMembers.length === 3, 'Set should have 3 members');

// Test ZADD/ZSCORE
db.zadd('myzset', [
  { score: 1.0, member: Buffer.from('one') },
  { score: 2.0, member: Buffer.from('two') },
]);
const score = db.zscore('myzset', Buffer.from('two'));
console.log('ZSCORE myzset two:', score);
console.assert(score === 2.0, 'Score should be 2.0');

// Test DBSIZE
const size = db.dbsize();
console.log('DBSIZE:', size);

// Test TYPE
const type = db.type('hello');
console.log('TYPE hello:', type);

console.log('\n✓ All tests passed!');
