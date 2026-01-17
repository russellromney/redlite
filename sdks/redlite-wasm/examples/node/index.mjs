/**
 * Redlite WASM - Node.js Example
 *
 * Demonstrates using Redlite WASM in a Node.js environment.
 * Build first with: wasm-pack build --target nodejs
 */

import { RedliteWasm } from '../../pkg/redlite_wasm.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function log(msg) {
    console.log(msg);
}

async function main() {
    log('=== Redlite WASM - Node.js Demo ===\n');

    // Create a new database
    const db = new RedliteWasm();
    log('Database created successfully!\n');

    // String Commands
    log('--- String Commands ---');
    db.set('greeting', encoder.encode('Hello, Redlite!'), null);
    const greeting = db.get('greeting');
    log(`GET greeting = "${decoder.decode(greeting)}"`);

    db.set('counter', encoder.encode('0'), null);
    log(`INCR counter = ${db.incr('counter')}`);
    log(`INCRBY counter 5 = ${db.incrby('counter', 5)}`);
    log(`DECR counter = ${db.decr('counter')}`);

    db.append('greeting', encoder.encode(' Welcome!'));
    log(`APPEND greeting -> "${decoder.decode(db.get('greeting'))}"`);
    log(`STRLEN greeting = ${db.strlen('greeting')}`);
    log('');

    // Key Commands
    log('--- Key Commands ---');
    db.set('key1', encoder.encode('value1'), null);
    db.set('key2', encoder.encode('value2'), null);
    db.set('other', encoder.encode('value3'), null);

    const keys = db.keys('key*');
    log(`KEYS key* = [${keys.join(', ')}]`);
    log(`EXISTS key1 key2 missing = ${db.exists(['key1', 'key2', 'missing'])}`);
    log(`TYPE key1 = ${db.type('key1')}`);
    log(`DBSIZE = ${db.dbsize()}`);

    db.expire('key1', 3600);
    log(`EXPIRE key1 3600 -> TTL = ${db.ttl('key1')}s`);
    log('');

    // Hash Commands
    log('--- Hash Commands ---');
    db.hset('user:100', 'name', encoder.encode('John Doe'));
    db.hset('user:100', 'email', encoder.encode('john@example.com'));
    db.hset('user:100', 'visits', encoder.encode('0'));

    log(`HGET user:100 name = "${decoder.decode(db.hget('user:100', 'name'))}"`);
    log(`HKEYS user:100 = [${db.hkeys('user:100').join(', ')}]`);
    log(`HINCRBY user:100 visits 1 = ${db.hincrby('user:100', 'visits', 1)}`);
    log(`HLEN user:100 = ${db.hlen('user:100')}`);
    log(`HEXISTS user:100 name = ${db.hexists('user:100', 'name')}`);
    log('');

    // List Commands
    log('--- List Commands ---');
    db.rpush('queue', [
        encoder.encode('task1'),
        encoder.encode('task2'),
        encoder.encode('task3'),
    ]);
    log('RPUSH queue task1 task2 task3');

    db.lpush('queue', [encoder.encode('urgent')]);
    log('LPUSH queue urgent');

    const queueItems = db.lrange('queue', 0, -1);
    log(`LRANGE queue 0 -1 = [${queueItems.map(i => decoder.decode(i)).join(', ')}]`);
    log(`LLEN queue = ${db.llen('queue')}`);

    const popped = db.lpop('queue', 1);
    log(`LPOP queue = "${decoder.decode(popped[0])}"`);
    log('');

    // Set Commands
    log('--- Set Commands ---');
    db.sadd('tags', [
        encoder.encode('redis'),
        encoder.encode('sqlite'),
        encoder.encode('wasm'),
        encoder.encode('rust'),
    ]);
    log('SADD tags redis sqlite wasm rust');

    const members = db.smembers('tags');
    log(`SMEMBERS tags = [${members.map(m => decoder.decode(m)).join(', ')}]`);
    log(`SCARD tags = ${db.scard('tags')}`);
    log(`SISMEMBER tags wasm = ${db.sismember('tags', encoder.encode('wasm'))}`);
    log(`SISMEMBER tags python = ${db.sismember('tags', encoder.encode('python'))}`);

    db.srem('tags', [encoder.encode('sqlite')]);
    log(`SREM tags sqlite -> SCARD = ${db.scard('tags')}`);
    log('');

    // Sorted Set Commands
    log('--- Sorted Set Commands ---');
    db.zadd('scores', [
        95.5, encoder.encode('Alice'),
        87.0, encoder.encode('Bob'),
        92.3, encoder.encode('Charlie'),
        88.8, encoder.encode('Diana'),
    ]);
    log('ZADD scores 95.5 Alice 87 Bob 92.3 Charlie 88.8 Diana');

    log(`ZCARD scores = ${db.zcard('scores')}`);
    log(`ZSCORE scores Alice = ${db.zscore('scores', encoder.encode('Alice'))}`);
    log(`ZRANK scores Bob = ${db.zrank('scores', encoder.encode('Bob'))}`);
    log(`ZCOUNT scores 88 95 = ${db.zcount('scores', 88, 95)}`);

    const ranking = db.zrange('scores', 0, -1, false);
    log(`ZRANGE scores 0 -1 = [${ranking.map(r => decoder.decode(r)).join(', ')}]`);

    db.zincrby('scores', 5.0, encoder.encode('Bob'));
    log(`ZINCRBY scores 5 Bob -> ${db.zscore('scores', encoder.encode('Bob'))}`);
    log('');

    // Database selection
    log('--- Database Selection ---');
    log(`Current DBSIZE = ${db.dbsize()}`);
    db.select(1);
    log(`SELECT 1 -> DBSIZE = ${db.dbsize()}`);
    db.set('db1_key', encoder.encode('in database 1'), null);
    log(`SET db1_key in database 1`);
    db.select(0);
    log(`SELECT 0 -> db1_key exists = ${db.exists(['db1_key'])}`);
    log('');

    log('=== Demo Complete ===');
}

main().catch(console.error);
