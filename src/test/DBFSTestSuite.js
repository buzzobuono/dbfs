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
                            { role: 'designer' },
                            { age: 29 }
                        ]
                    },
                    {
                        $and: [
                            { 'role': 'developer' },
                            { age: 33 }
                        ]
                    }
                ]
            },
            { active: true }
        ]
    },
    //like: { name: '*3335*' },
    //filter: { active: true },
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
        { active: true },
            { role: 'developer' }
        //    { age: 33 },
            
        ]
    },
    //like: { name: '*3335*' },
    //filter: { role: 'manager'},
    orderBy: 'id asc',
    offset: 0,
    limit: 2
};

//var stats = await users.getStats();
//console.log(JSON.stringify(stats, null, 2));

console.time('timer1');
var results = await users.find(query3);
console.timeEnd('timer1');
console.log(results);
console.log('----------------------------------\n');
