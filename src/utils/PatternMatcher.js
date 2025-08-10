class PatternMatcher {
  static analyzePattern(pattern, caseSensitive = false) {
    const info = {
      originalPattern: pattern,
      caseSensitive: caseSensitive,
      canUseIndex: false,
      optimizationType: null,
      prefixPattern: null,
      regex: null,
      indexable: false
    };

    const normalizedPattern = caseSensitive ? pattern : pattern.toLowerCase();
    
    if (normalizedPattern.includes('%') || normalizedPattern.includes('_')) {
      info.optimizationType = 'SQL_WILDCARDS';
      info.regex = this.sqlToRegex(normalizedPattern, caseSensitive);
      
      if (normalizedPattern.endsWith('%') && !normalizedPattern.slice(0, -1).includes('%') && !normalizedPattern.slice(0, -1).includes('_')) {
        info.canUseIndex = true;
        info.indexable = true;
        info.prefixPattern = normalizedPattern.slice(0, -1);
      }
    } else if (normalizedPattern.includes('*') || normalizedPattern.includes('?')) {
      info.optimizationType = 'UNIX_WILDCARDS';
      info.regex = this.unixToRegex(normalizedPattern, caseSensitive);
      
      if (normalizedPattern.endsWith('*') && !normalizedPattern.slice(0, -1).includes('*') && !normalizedPattern.slice(0, -1).includes('?')) {
        info.canUseIndex = true;
        info.indexable = true;
        info.prefixPattern = normalizedPattern.slice(0, -1);
      }
    } else {
      info.optimizationType = 'REGEX';
      try {
        info.regex = new RegExp(normalizedPattern, caseSensitive ? 'g' : 'gi');
      } catch (e) {
        const escapedPattern = normalizedPattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        info.regex = new RegExp(escapedPattern, caseSensitive ? 'g' : 'gi');
      }
    }

    return info;
  }

  static sqlToRegex(pattern, caseSensitive) {
    let regexPattern = pattern
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      .replace(/\\%/g, '.*')
      .replace(/\\_/g, '.');
    
    const flags = caseSensitive ? '' : 'i';
    return new RegExp(`^${regexPattern}$`, flags);
  }

  static unixToRegex(pattern, caseSensitive) {
    let regexPattern = pattern
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      .replace(/\\\*/g, '.*')
      .replace(/\\\?/g, '.');
    
    const flags = caseSensitive ? '' : 'i';
    return new RegExp(`^${regexPattern}$`, flags);
  }

  static documentMatches(doc, field, patternInfo) {
    const value = doc[field];
    if (value === undefined || value === null) return false;
    
    if (Array.isArray(value)) {
      return value.some(item => patternInfo.regex.test(String(item)));
    }
    
    if (field.includes('.')) {
      const nestedValue = this.getNestedValue(doc, field);
      if (nestedValue === undefined || nestedValue === null) return false;
      return patternInfo.regex.test(String(nestedValue));
    }
    
    return patternInfo.regex.test(String(value));
  }

  static getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => current && current[key], obj);
  }
}

export default PatternMatcher;
