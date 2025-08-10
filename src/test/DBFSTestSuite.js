import UnifiedFSDB from '../core/UnifiedFSDB.js';
import fs from 'fs';
import readline from 'readline/promises';
import { stdin as input, stdout as output } from 'node:process';

const db = await UnifiedFSDB.open('./test_data'); 

const users = await db.collection('users');


/*const results = await users.findByRange('age', 25, 27);
console.log('✅ Range query:', results.length, 'users aged 25-27');
*/

const query = {
    whereComplex: {
        $and: [
        {
            $or: [
            {
                $and: [
                { role: 'developer'},
                { age: 21 }
                ]
            },
            {
                $and: [
                { 'role': 'manager' },
                { age: 38 }
                ]
            }
            ]
        },
        { active: false}
        ]
    },
    // whereLike: { name: 'Chris%' },
    orderBy: 'name asc',
    limit: 5
};

//var stats = await users.getStats();
//console.log(JSON.stringify(stats, null, 2));

console.time('timer1');
var results = await users.findAdvanced(query);
console.timeEnd('timer1');
//console.log(results);
console.log('----------------------------------\n');

//stats = await users.getStats();
//console.log(JSON.stringify(stats, null, 2));

console.time('timer2');
results = await users.findAdvanced(query);
console.timeEnd('timer2');
//console.log(results);
console.log('----------------------------------\n');

process.exit();

const rl = readline.createInterface({ input, output });

while (true) {
    const key = await rl.question('Inserisci la chiave (es. email, age, name, active, role, skills): ');
    let value = await rl.question('Inserisci il valore: ');
    
    if (key === 'age') {
        value = Number(value);
    } else if (key === 'active') {
        value = value.toLowerCase() === 'true';
    }
    
    const results = await users.findByField(key, value);
    console.log('\n' + key + ': ' + value);
    console.log(results);
    console.log('----------------------------------\n');
}

