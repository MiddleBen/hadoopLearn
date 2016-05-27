/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package avro.entity;  
@SuppressWarnings("all")
/** a pair of strings */
@org.apache.avro.specific.AvroGenerated
public class StringPair extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"StringPair\",\"namespace\":\"ch04.entity\",\"doc\":\"a pair of strings\",\"fields\":[{\"name\":\"right\",\"type\":\"string\",\"order\":\"descending\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence right;

  /**
   * Default constructor.
   */
  public StringPair() {}

  /**
   * All-args constructor.
   */
  public StringPair(java.lang.CharSequence right) {
    this.right = right;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return right;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: right = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'right' field.
   */
  public java.lang.CharSequence getRight() {
    return right;
  }

  /**
   * Sets the value of the 'right' field.
   * @param value the value to set.
   */
  public void setRight(java.lang.CharSequence value) {
    this.right = value;
  }

  /** Creates a new StringPair RecordBuilder */
  public static avro.entity.StringPair.Builder newBuilder() {
    return new avro.entity.StringPair.Builder();
  }
  
  /** Creates a new StringPair RecordBuilder by copying an existing Builder */
  public static avro.entity.StringPair.Builder newBuilder(avro.entity.StringPair.Builder other) {
    return new avro.entity.StringPair.Builder(other);
  }
  
  /** Creates a new StringPair RecordBuilder by copying an existing StringPair instance */
  public static avro.entity.StringPair.Builder newBuilder(avro.entity.StringPair other) {
    return new avro.entity.StringPair.Builder(other);
  }
  
  /**
   * RecordBuilder for StringPair instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<StringPair>
    implements org.apache.avro.data.RecordBuilder<StringPair> {

    private java.lang.CharSequence right;

    /** Creates a new Builder */
    private Builder() {
      super(avro.entity.StringPair.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(avro.entity.StringPair.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing StringPair instance */
    private Builder(avro.entity.StringPair other) {
            super(avro.entity.StringPair.SCHEMA$);
      if (isValidValue(fields()[0], other.right)) {
        this.right = data().deepCopy(fields()[0].schema(), other.right);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'right' field */
    public java.lang.CharSequence getRight() {
      return right;
    }
    
    /** Sets the value of the 'right' field */
    public avro.entity.StringPair.Builder setRight(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.right = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'right' field has been set */
    public boolean hasRight() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'right' field */
    public avro.entity.StringPair.Builder clearRight() {
      right = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public StringPair build() {
      try {
        StringPair record = new StringPair();
        record.right = fieldSetFlags()[0] ? this.right : (java.lang.CharSequence) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}