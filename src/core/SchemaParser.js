import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';


class SchemaParser {
  static parseSchema(schema) {
    return {
      fields: schema.fields || {},
      relations: this.parseRelations(schema.relations || {}),
      validateRelations: schema.validateRelations || false,
      indices: schema.indices || {}
    };
  }

  static parseRelations(relations) {
    const parsed = {};
    for (const [field, relation] of Object.entries(relations)) {
      if (typeof relation === 'string') {
        parsed[field] = { collection: relation, field: 'id' };
      } else {
        parsed[field] = relation;
      }
    }
    return parsed;
  }

  static validateDocument(doc, schema) {
    const errors = [];
    
    for (const [fieldName, fieldDef] of Object.entries(schema.fields)) {
      const isRequired = fieldDef.required || (typeof fieldDef === 'object' && fieldDef.required);
      
      if (isRequired && (doc[fieldName] === undefined || doc[fieldName] === null)) {
        errors.push(`Missing required field: ${fieldName}`);
      }
      
      if (doc[fieldName] !== undefined && typeof fieldDef === 'object' && fieldDef.type) {
        if (!this.validateFieldType(doc[fieldName], fieldDef.type)) {
          errors.push(`Invalid type for field ${fieldName}: expected ${fieldDef.type}`);
        }
      }
    }
    
    return errors;
  }

  static validateFieldType(value, expectedType) {
    if (value === null || value === undefined) return true;
    
    switch (expectedType) {
      case 'string': return typeof value === 'string';
      case 'number': return typeof value === 'number';
      case 'boolean': return typeof value === 'boolean';
      case 'array': return Array.isArray(value);
      case 'object': return typeof value === 'object' && !Array.isArray(value);
      case 'date': return value instanceof Date || !isNaN(Date.parse(value));
      default: return true;
    }
  }

  static async validateRelations(doc, schema, db) {
    const errors = [];
    
    for (const [field, relation] of Object.entries(schema.relations)) {
      const value = doc[field];
      if (value) {
        const targetCollection = db.collection(relation.collection);
        const exists = await targetCollection.getById(value);
        if (!exists) {
          errors.push(`Invalid relation: ${field}=${value} not found in ${relation.collection}`);
        }
      }
    }
    
    return errors;
  }

  static getSingleFieldIndexedNames(schema) {
    const singleFieldIndexNames = new Map();
    
    for (const [indexName, fields] of Object.entries(schema.indices)) {
      if (fields.length == 1) {
        singleFieldIndexNames.set(fields[0], indexName);
      }
    }
    console.log(singleFieldIndexNames)
    return singleFieldIndexNames;
  }
  
  
}


export default SchemaParser;

