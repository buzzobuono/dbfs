

class ValueNormalizer {
  static normalize(value) {
    if (typeof value === 'string') return value.toLowerCase().trim();
    if (typeof value === 'number') return value.toString();
    if (typeof value === 'boolean') return value.toString();
    if (value instanceof Date) return value.toISOString();
    if (value === null || value === undefined) return null;
    return String(value);
  }
}

export default ValueNormalizer;

