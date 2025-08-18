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
        $and: [
            {
                $or: [
                    {
                        $and: [
                            { age: 29 },
                            { role: 'designer' },
                            { active: true }
                        ]
                    },
                    {
                        $and: [
                            { age: 33 },
                            { 'role': 'developer' },
                            { active: true },
                           // { name: 'Alex Martin 60'}
                        ]
                    }
                ]
            }
            
        ]
    },
    like: { name: '*martin*' },
    filter: { active: true },
    orderBy: 'name desc',
    limit: 6
};


const query1 = {
    where: {
        $and: [
            // { active: false },
            { role: 'designer' },
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
    filter: { active: 'true' },
    orderBy: 'name asc',
    //offset: 11,
    limit: 5
};

const query3 = {
    where: {
        $and: [
        { age: 33 },
        { role: 'developer' },
        { active: true },
        { name: 'Alex Martin 60'},
        ]
    },
    like: { name: '*60*' },
    //filter: { role: 'manager'},
    orderBy: 'id asc',
    offset: 0,
    limit: 2
};

//var stats = await users.getStats();
//console.log(JSON.stringify(stats, null, 2));

console.time('timer1');
var results = await users.find(query);
console.timeEnd('timer1');
console.log(results);
console.log('----------------------------------\n');
