

class RelationEngine {
  constructor(collection) {
    this.collection = collection;
  }

  async populate(docs, ...relationNames) {
    if (!Array.isArray(docs)) docs = [docs];
    if (docs.length === 0) return docs;
    
    const populated = [];
    for (const doc of docs) {
      const populatedDoc = { ...doc };
      
      for (const relationName of relationNames) {
        if (this.collection.schema.relations[relationName]) {
          const relation = this.collection.schema.relations[relationName];
          const foreignValue = doc[relationName];
          
          if (foreignValue) {
            const targetCollection = this.collection.db.collection(relation.collection);
            const related = await targetCollection.getById(foreignValue);
            
            const fieldName = relationName.endsWith('Id') 
              ? relationName.slice(0, -2) 
              : relationName + '_data';
            populatedDoc[fieldName] = related;
          }
        }
      }
      
      populated.push(populatedDoc);
    }
    
    return Array.isArray(arguments[0]) ? populated : populated[0];
  }

  async _populateRelation(fkField, fkValue, deep = false) {
    const relation = this.collection.schema.relations[fkField];
    if (!relation) return null;
    
    const targetCollection = this.collection.db.collection(relation.collection);
    const related = await targetCollection.getById(fkValue);
    
    if (deep && related) {
      return await this._deepPopulate(related);
    }
    
    return related;
  }

  async _deepPopulate(doc) {
    const populated = { ...doc };
    
    for (const fkField of Object.keys(this.collection.schema.relations)) {
      const fkValue = doc[fkField];
      if (fkValue) {
        const related = await this._populateRelation(fkField, fkValue, false);
        if (related) {
          const relationName = fkField.replace(/Id$/, '');
          populated[relationName] = related;
        }
      }
    }
    
    return populated;
  }

  async join(targetCollectionName, localField, options = {}) {
    const { where = {}, limit = 1000, batchSize = 100 } = options;
    
    if (!this.collection.schema.relations[localField]) {
      console.warn(`⚠️ Field ${localField} is not defined as a relation in schema`);
    }
    
    const localDocs = await this._getFilteredDocs(where, limit);
    if (localDocs.length === 0) return localDocs;
    
    const foreignValues = [...new Set(
      localDocs
        .map(doc => doc[localField])
        .filter(val => val != null)
    )];
    
    if (foreignValues.length === 0) return localDocs;
    
    const targetCollection = this.collection.db.collection(targetCollectionName);
    const relatedMap = await this._batchLoadRelated(
      targetCollection, 
      'id',
      foreignValues, 
      batchSize
    );
    
    const relationName = localField.endsWith('Id') 
      ? localField.slice(0, -2) 
      : localField + '_data';
      
    return localDocs.map(doc => {
      const foreignValue = doc[localField];
      const related = relatedMap.get(foreignValue);
      return { ...doc, [relationName]: related || null };
    });
  }

  async _batchLoadRelated(targetCollection, field, values, batchSize) {
    const relatedMap = new Map();
    
    for (let i = 0; i < values.length; i += batchSize) {
      const batch = values.slice(i, i + batchSize);
      
      const loadPromises = batch.map(async value => {
        const doc = await targetCollection.getById(value);
        if (doc) relatedMap.set(value, doc);
      });
      
      await Promise.all(loadPromises);
    }
    
    return relatedMap;
  }

  async _getFilteredDocs(where, limit) {
    if (Object.keys(where).length === 0) {
      return await this._getFirstNDocs(limit);
    }
    
    const field = Object.keys(where)[0];
    const value = where[field];
    return await this.collection.findByField(field, value, { limit });
  }

  async _getFirstNDocs(limit) {
    const docs = await this.collection.storage.getAllDocuments();
    return docs.slice(0, limit);
  }

  async getRelated(id, targetCollectionName, targetField) {
    const targetCollection = this.collection.db.collection(targetCollectionName);
    return await targetCollection.findByField(targetField, id);
  }

  async countRelated(id, targetCollectionName, targetField) {
    const related = await this.getRelated(id, targetCollectionName, targetField);
    return related.length;
  }
}

export default RelationEngine;
