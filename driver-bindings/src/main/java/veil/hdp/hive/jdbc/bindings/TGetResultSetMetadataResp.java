/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package veil.hdp.hive.jdbc.bindings;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TGetResultSetMetadataResp implements org.apache.thrift.TBase<TGetResultSetMetadataResp, TGetResultSetMetadataResp._Fields>, java.io.Serializable, Cloneable, Comparable<TGetResultSetMetadataResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TGetResultSetMetadataResp");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField SCHEMA_FIELD_DESC = new org.apache.thrift.protocol.TField("schema", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TGetResultSetMetadataRespStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TGetResultSetMetadataRespTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable TStatus status; // required
  private @org.apache.thrift.annotation.Nullable TTableSchema schema; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    SCHEMA((short)2, "schema");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // STATUS
          return STATUS;
        case 2: // SCHEMA
          return SCHEMA;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.SCHEMA};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TStatus.class)));
    tmpMap.put(_Fields.SCHEMA, new org.apache.thrift.meta_data.FieldMetaData("schema", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TTableSchema.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TGetResultSetMetadataResp.class, metaDataMap);
  }

  public TGetResultSetMetadataResp() {
  }

  public TGetResultSetMetadataResp(
    TStatus status)
  {
    this();
    this.status = status;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TGetResultSetMetadataResp(TGetResultSetMetadataResp other) {
    if (other.isSetStatus()) {
      this.status = new TStatus(other.status);
    }
    if (other.isSetSchema()) {
      this.schema = new TTableSchema(other.schema);
    }
  }

  @Override
  public TGetResultSetMetadataResp deepCopy() {
    return new TGetResultSetMetadataResp(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.schema = null;
  }

  @org.apache.thrift.annotation.Nullable
  public TStatus getStatus() {
    return this.status;
  }

  public void setStatus(@org.apache.thrift.annotation.Nullable TStatus status) {
    this.status = status;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public TTableSchema getSchema() {
    return this.schema;
  }

  public void setSchema(@org.apache.thrift.annotation.Nullable TTableSchema schema) {
    this.schema = schema;
  }

  public void unsetSchema() {
    this.schema = null;
  }

  /** Returns true if field schema is set (has been assigned a value) and false otherwise */
  public boolean isSetSchema() {
    return this.schema != null;
  }

  public void setSchemaIsSet(boolean value) {
    if (!value) {
      this.schema = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((TStatus)value);
      }
      break;

    case SCHEMA:
      if (value == null) {
        unsetSchema();
      } else {
        setSchema((TTableSchema)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case SCHEMA:
      return getSchema();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case SCHEMA:
      return isSetSchema();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TGetResultSetMetadataResp)
      return this.equals((TGetResultSetMetadataResp)that);
    return false;
  }

  public boolean equals(TGetResultSetMetadataResp that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_schema = true && this.isSetSchema();
    boolean that_present_schema = true && that.isSetSchema();
    if (this_present_schema || that_present_schema) {
      if (!(this_present_schema && that_present_schema))
        return false;
      if (!this.schema.equals(that.schema))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetStatus()) ? 131071 : 524287);
    if (isSetStatus())
      hashCode = hashCode * 8191 + status.hashCode();

    hashCode = hashCode * 8191 + ((isSetSchema()) ? 131071 : 524287);
    if (isSetSchema())
      hashCode = hashCode * 8191 + schema.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TGetResultSetMetadataResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetStatus(), other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSchema(), other.isSetSchema());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSchema()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.schema, other.schema);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TGetResultSetMetadataResp(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (isSetSchema()) {
      if (!first) sb.append(", ");
      sb.append("schema:");
      if (this.schema == null) {
        sb.append("null");
      } else {
        sb.append(this.schema);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetStatus()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (status != null) {
      status.validate();
    }
    if (schema != null) {
      schema.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TGetResultSetMetadataRespStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TGetResultSetMetadataRespStandardScheme getScheme() {
      return new TGetResultSetMetadataRespStandardScheme();
    }
  }

  private static class TGetResultSetMetadataRespStandardScheme extends org.apache.thrift.scheme.StandardScheme<TGetResultSetMetadataResp> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TGetResultSetMetadataResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.status = new TStatus();
              struct.status.read(iprot);
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SCHEMA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.schema = new TTableSchema();
              struct.schema.read(iprot);
              struct.setSchemaIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, TGetResultSetMetadataResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.schema != null) {
        if (struct.isSetSchema()) {
          oprot.writeFieldBegin(SCHEMA_FIELD_DESC);
          struct.schema.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TGetResultSetMetadataRespTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TGetResultSetMetadataRespTupleScheme getScheme() {
      return new TGetResultSetMetadataRespTupleScheme();
    }
  }

  private static class TGetResultSetMetadataRespTupleScheme extends org.apache.thrift.scheme.TupleScheme<TGetResultSetMetadataResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TGetResultSetMetadataResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.status.write(oprot);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetSchema()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetSchema()) {
        struct.schema.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TGetResultSetMetadataResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.status = new TStatus();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.schema = new TTableSchema();
        struct.schema.read(iprot);
        struct.setSchemaIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

