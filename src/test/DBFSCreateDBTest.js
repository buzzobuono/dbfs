import UnifiedFSDB from '../core/UnifiedFSDB.js';
import fs from 'fs';

if (fs.existsSync('./test_data')) {
  fs.rmSync('./test_data', { recursive: true, force: true });
}

const db = await UnifiedFSDB.create('./test_data');

// Users collection
const users = await db.collection('users', {
  fields: {
    name: { type: 'string', required: true },
    email: { type: 'string', required: true },
    age: { type: 'number' },
    role: { type: 'string' },
    skills: { type: 'array' },
    active: { type: 'boolean', required: true },
    phone: { type: 'string' }
  },
  indices: { 
    'age': ['age'],
    'role': ['role'],
    'name': ['name'],
    'active': ['active'],
    'age_role_active' : [ 'age', 'role', 'active']
  },
  maxPerDir: 10
});

const roles = ['developer', 'designer', 'manager'];
const skillsByRole = {
  developer: ['javascript', 'node', 'react', 'angular', 'vue', 'spring', 'typescript'],
  designer: ['design', 'figma', 'css', 'illustrator', 'photoshop'],
  manager: ['leadership', 'planning', 'scrum', 'kanban']
};

function getRandomElement(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function getRandomSkills(role) {
  const possibleSkills = skillsByRole[role];
  const numSkills = Math.floor(Math.random() * 3) + 2; // 2-4 skills
  const shuffled = [...possibleSkills].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, numSkills);
}

function generateName(index) {
  const firstNames = ['Alex', 'Sam', 'Chris', 'Taylor', 'Jordan', 'Morgan', 'Jamie', 'Casey', 'Riley', 'Avery'];
  const lastNames = ['Smith', 'Johnson', 'Brown', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White', 'Harris', 'Martin'];
  return `${getRandomElement(firstNames)} ${getRandomElement(lastNames)} ${index}`;
}

for (let i = 1; i <= 100; i++) {
  const role = getRandomElement(roles);
  const name = generateName(i);
  const email = name.toLowerCase().replace(/\s+/g, '.').replace(/_/g, '') + '@test.com';
  const age = Math.floor(Math.random() * 30) + 20; // Età tra 20 e 49
  const skills = getRandomSkills(role);
  const active = Math.random() < 0.8; // 80% attivi

  let user = {
    name,
    email,
    age,
    role,
    skills,
    active
  };
  console.log(user);
  await users.insert(user, { updateIndices: false });
}

await users.buildAllIndices();
await db.close();

