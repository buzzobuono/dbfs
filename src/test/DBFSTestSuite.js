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
    where: {
      //  $and: [
      //  {
            $or: [
            {
                $and: [
                { role: 'manager'},
                { age: 46 }
                ]
            },
            {
                $and: [
                { 'role': 'developer' },
                { age: 21 }
                ]
            },
            { name: 'Chris White 156'}
            ]
    //    },
       // { active: false}
   //     ]
    },
   // like: { name: 'c*' },
    filter: { active: true },
    orderBy: 'name desc',
    limit: 2
};


const query1 = {
    where: {
        $and: [
        // { active: false },
        { role: 'designer'},
        // { age: 36 }
        ]
    },
    like: { name: 'c*' },
 //   orderBy: 'name asc',
    limit: 5
};
const query2 = {
    where: {
        role: 'designer',
        age: 36
    },
    like: { name: 'c*' },
    filter : { active: 'true'},
    orderBy: 'name asc',
    //offset: 11,
    limit: 5
};

var stats = await users.getStats();
console.log(JSON.stringify(stats, null, 2));

console.time('timer1');
var results = await users.find(query);
console.timeEnd('timer1');
console.log(results);
console.log('----------------------------------\n');
/*
var stats = await users.getStats();
console.log(JSON.stringify(stats, null, 2));

console.time('timer2');
results = await users.find(query);
console.timeEnd('timer2');
console.log(results);
console.log('----------------------------------\n');
*/

/*console.time('timer3');
results = await users.findByFullText('name', 'casey smith');
console.timeEnd('timer3');
console.log(results);
console.log('----------------------------------\n');
*/
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

