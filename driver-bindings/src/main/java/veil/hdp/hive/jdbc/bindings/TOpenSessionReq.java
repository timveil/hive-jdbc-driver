/**
 * Autogenerated by Thrift Compiler (0.19.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package veil.hdp.hive.jdbc.bindings;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TOpenSessionReq implements org.apache.thrift.TBase<TOpenSessionReq, TOpenSessionReq._Fields>, java.io.Serializable, Cloneable, Comparable<TOpenSessionReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TOpenSessionReq");

  private static final org.apache.thrift.protocol.TField CLIENT_PROTOCOL_FIELD_DESC = new org.apache.thrift.protocol.TField("client_protocol", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField USERNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("username", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField PASSWORD_FIELD_DESC = new org.apache.thrift.protocol.TField("password", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField CONFIGURATION_FIELD_DESC = new org.apache.thrift.protocol.TField("configuration", org.apache.thrift.protocol.TType.MAP, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TOpenSessionReqStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TOpenSessionReqTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable TProtocolVersion client_protocol; // required
  private @org.apache.thrift.annotation.Nullable java.lang.String username; // optional
  private @org.apache.thrift.annotation.Nullable java.lang.String password; // optional
  private @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> configuration; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see TProtocolVersion
     */
    CLIENT_PROTOCOL((short)1, "client_protocol"),
    USERNAME((short)2, "username"),
    PASSWORD((short)3, "password"),
    CONFIGURATION((short)4, "configuration");

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
        case 1: // CLIENT_PROTOCOL
          return CLIENT_PROTOCOL;
        case 2: // USERNAME
          return USERNAME;
        case 3: // PASSWORD
          return PASSWORD;
        case 4: // CONFIGURATION
          return CONFIGURATION;
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
  private static final _Fields optionals[] = {_Fields.USERNAME,_Fields.PASSWORD,_Fields.CONFIGURATION};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CLIENT_PROTOCOL, new org.apache.thrift.meta_data.FieldMetaData("client_protocol", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TProtocolVersion.class)));
    tmpMap.put(_Fields.USERNAME, new org.apache.thrift.meta_data.FieldMetaData("username", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PASSWORD, new org.apache.thrift.meta_data.FieldMetaData("password", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONFIGURATION, new org.apache.thrift.meta_data.FieldMetaData("configuration", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TOpenSessionReq.class, metaDataMap);
  }

  public TOpenSessionReq() {
    this.client_protocol = veil.hdp.hive.jdbc.bindings.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;

  }

  public TOpenSessionReq(
    TProtocolVersion client_protocol)
  {
    this();
    this.client_protocol = client_protocol;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TOpenSessionReq(TOpenSessionReq other) {
    if (other.isSetClient_protocol()) {
      this.client_protocol = other.client_protocol;
    }
    if (other.isSetUsername()) {
      this.username = other.username;
    }
    if (other.isSetPassword()) {
      this.password = other.password;
    }
    if (other.isSetConfiguration()) {
      java.util.Map<java.lang.String,java.lang.String> __this__configuration = new java.util.HashMap<java.lang.String,java.lang.String>(other.configuration);
      this.configuration = __this__configuration;
    }
  }

  @Override
  public TOpenSessionReq deepCopy() {
    return new TOpenSessionReq(this);
  }

  @Override
  public void clear() {
    this.client_protocol = veil.hdp.hive.jdbc.bindings.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;

    this.username = null;
    this.password = null;
    this.configuration = null;
  }

  /**
   * 
   * @see TProtocolVersion
   */
  @org.apache.thrift.annotation.Nullable
  public TProtocolVersion getClient_protocol() {
    return this.client_protocol;
  }

  /**
   * 
   * @see TProtocolVersion
   */
  public void setClient_protocol(@org.apache.thrift.annotation.Nullable TProtocolVersion client_protocol) {
    this.client_protocol = client_protocol;
  }

  public void unsetClient_protocol() {
    this.client_protocol = null;
  }

  /** Returns true if field client_protocol is set (has been assigned a value) and false otherwise */
  public boolean isSetClient_protocol() {
    return this.client_protocol != null;
  }

  public void setClient_protocolIsSet(boolean value) {
    if (!value) {
      this.client_protocol = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getUsername() {
    return this.username;
  }

  public void setUsername(@org.apache.thrift.annotation.Nullable java.lang.String username) {
    this.username = username;
  }

  public void unsetUsername() {
    this.username = null;
  }

  /** Returns true if field username is set (has been assigned a value) and false otherwise */
  public boolean isSetUsername() {
    return this.username != null;
  }

  public void setUsernameIsSet(boolean value) {
    if (!value) {
      this.username = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getPassword() {
    return this.password;
  }

  public void setPassword(@org.apache.thrift.annotation.Nullable java.lang.String password) {
    this.password = password;
  }

  public void unsetPassword() {
    this.password = null;
  }

  /** Returns true if field password is set (has been assigned a value) and false otherwise */
  public boolean isSetPassword() {
    return this.password != null;
  }

  public void setPasswordIsSet(boolean value) {
    if (!value) {
      this.password = null;
    }
  }

  public int getConfigurationSize() {
    return (this.configuration == null) ? 0 : this.configuration.size();
  }

  public void putToConfiguration(java.lang.String key, java.lang.String val) {
    if (this.configuration == null) {
      this.configuration = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.configuration.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getConfiguration() {
    return this.configuration;
  }

  public void setConfiguration(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> configuration) {
    this.configuration = configuration;
  }

  public void unsetConfiguration() {
    this.configuration = null;
  }

  /** Returns true if field configuration is set (has been assigned a value) and false otherwise */
  public boolean isSetConfiguration() {
    return this.configuration != null;
  }

  public void setConfigurationIsSet(boolean value) {
    if (!value) {
      this.configuration = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case CLIENT_PROTOCOL:
      if (value == null) {
        unsetClient_protocol();
      } else {
        setClient_protocol((TProtocolVersion)value);
      }
      break;

    case USERNAME:
      if (value == null) {
        unsetUsername();
      } else {
        setUsername((java.lang.String)value);
      }
      break;

    case PASSWORD:
      if (value == null) {
        unsetPassword();
      } else {
        setPassword((java.lang.String)value);
      }
      break;

    case CONFIGURATION:
      if (value == null) {
        unsetConfiguration();
      } else {
        setConfiguration((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case CLIENT_PROTOCOL:
      return getClient_protocol();

    case USERNAME:
      return getUsername();

    case PASSWORD:
      return getPassword();

    case CONFIGURATION:
      return getConfiguration();

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
    case CLIENT_PROTOCOL:
      return isSetClient_protocol();
    case USERNAME:
      return isSetUsername();
    case PASSWORD:
      return isSetPassword();
    case CONFIGURATION:
      return isSetConfiguration();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TOpenSessionReq)
      return this.equals((TOpenSessionReq)that);
    return false;
  }

  public boolean equals(TOpenSessionReq that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_client_protocol = true && this.isSetClient_protocol();
    boolean that_present_client_protocol = true && that.isSetClient_protocol();
    if (this_present_client_protocol || that_present_client_protocol) {
      if (!(this_present_client_protocol && that_present_client_protocol))
        return false;
      if (!this.client_protocol.equals(that.client_protocol))
        return false;
    }

    boolean this_present_username = true && this.isSetUsername();
    boolean that_present_username = true && that.isSetUsername();
    if (this_present_username || that_present_username) {
      if (!(this_present_username && that_present_username))
        return false;
      if (!this.username.equals(that.username))
        return false;
    }

    boolean this_present_password = true && this.isSetPassword();
    boolean that_present_password = true && that.isSetPassword();
    if (this_present_password || that_present_password) {
      if (!(this_present_password && that_present_password))
        return false;
      if (!this.password.equals(that.password))
        return false;
    }

    boolean this_present_configuration = true && this.isSetConfiguration();
    boolean that_present_configuration = true && that.isSetConfiguration();
    if (this_present_configuration || that_present_configuration) {
      if (!(this_present_configuration && that_present_configuration))
        return false;
      if (!this.configuration.equals(that.configuration))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetClient_protocol()) ? 131071 : 524287);
    if (isSetClient_protocol())
      hashCode = hashCode * 8191 + client_protocol.getValue();

    hashCode = hashCode * 8191 + ((isSetUsername()) ? 131071 : 524287);
    if (isSetUsername())
      hashCode = hashCode * 8191 + username.hashCode();

    hashCode = hashCode * 8191 + ((isSetPassword()) ? 131071 : 524287);
    if (isSetPassword())
      hashCode = hashCode * 8191 + password.hashCode();

    hashCode = hashCode * 8191 + ((isSetConfiguration()) ? 131071 : 524287);
    if (isSetConfiguration())
      hashCode = hashCode * 8191 + configuration.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TOpenSessionReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetClient_protocol(), other.isSetClient_protocol());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClient_protocol()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.client_protocol, other.client_protocol);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetUsername(), other.isSetUsername());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUsername()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.username, other.username);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetPassword(), other.isSetPassword());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPassword()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.password, other.password);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetConfiguration(), other.isSetConfiguration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConfiguration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.configuration, other.configuration);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TOpenSessionReq(");
    boolean first = true;

    sb.append("client_protocol:");
    if (this.client_protocol == null) {
      sb.append("null");
    } else {
      sb.append(this.client_protocol);
    }
    first = false;
    if (isSetUsername()) {
      if (!first) sb.append(", ");
      sb.append("username:");
      if (this.username == null) {
        sb.append("null");
      } else {
        sb.append(this.username);
      }
      first = false;
    }
    if (isSetPassword()) {
      if (!first) sb.append(", ");
      sb.append("password:");
      if (this.password == null) {
        sb.append("null");
      } else {
        sb.append(this.password);
      }
      first = false;
    }
    if (isSetConfiguration()) {
      if (!first) sb.append(", ");
      sb.append("configuration:");
      if (this.configuration == null) {
        sb.append("null");
      } else {
        sb.append(this.configuration);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetClient_protocol()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'client_protocol' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
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

  private static class TOpenSessionReqStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TOpenSessionReqStandardScheme getScheme() {
      return new TOpenSessionReqStandardScheme();
    }
  }

  private static class TOpenSessionReqStandardScheme extends org.apache.thrift.scheme.StandardScheme<TOpenSessionReq> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CLIENT_PROTOCOL
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.client_protocol = veil.hdp.hive.jdbc.bindings.TProtocolVersion.findByValue(iprot.readI32());
              struct.setClient_protocolIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // USERNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.username = iprot.readString();
              struct.setUsernameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PASSWORD
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.password = iprot.readString();
              struct.setPasswordIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CONFIGURATION
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map142 = iprot.readMapBegin();
                struct.configuration = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map142.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key143;
                @org.apache.thrift.annotation.Nullable java.lang.String _val144;
                for (int _i145 = 0; _i145 < _map142.size; ++_i145)
                {
                  _key143 = iprot.readString();
                  _val144 = iprot.readString();
                  struct.configuration.put(_key143, _val144);
                }
                iprot.readMapEnd();
              }
              struct.setConfigurationIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TOpenSessionReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.client_protocol != null) {
        oprot.writeFieldBegin(CLIENT_PROTOCOL_FIELD_DESC);
        oprot.writeI32(struct.client_protocol.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.username != null) {
        if (struct.isSetUsername()) {
          oprot.writeFieldBegin(USERNAME_FIELD_DESC);
          oprot.writeString(struct.username);
          oprot.writeFieldEnd();
        }
      }
      if (struct.password != null) {
        if (struct.isSetPassword()) {
          oprot.writeFieldBegin(PASSWORD_FIELD_DESC);
          oprot.writeString(struct.password);
          oprot.writeFieldEnd();
        }
      }
      if (struct.configuration != null) {
        if (struct.isSetConfiguration()) {
          oprot.writeFieldBegin(CONFIGURATION_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.configuration.size()));
            for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter146 : struct.configuration.entrySet())
            {
              oprot.writeString(_iter146.getKey());
              oprot.writeString(_iter146.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TOpenSessionReqTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TOpenSessionReqTupleScheme getScheme() {
      return new TOpenSessionReqTupleScheme();
    }
  }

  private static class TOpenSessionReqTupleScheme extends org.apache.thrift.scheme.TupleScheme<TOpenSessionReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.client_protocol.getValue());
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetUsername()) {
        optionals.set(0);
      }
      if (struct.isSetPassword()) {
        optionals.set(1);
      }
      if (struct.isSetConfiguration()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetUsername()) {
        oprot.writeString(struct.username);
      }
      if (struct.isSetPassword()) {
        oprot.writeString(struct.password);
      }
      if (struct.isSetConfiguration()) {
        {
          oprot.writeI32(struct.configuration.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter147 : struct.configuration.entrySet())
          {
            oprot.writeString(_iter147.getKey());
            oprot.writeString(_iter147.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TOpenSessionReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.client_protocol = veil.hdp.hive.jdbc.bindings.TProtocolVersion.findByValue(iprot.readI32());
      struct.setClient_protocolIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.username = iprot.readString();
        struct.setUsernameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.password = iprot.readString();
        struct.setPasswordIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map148 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
          struct.configuration = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map148.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key149;
          @org.apache.thrift.annotation.Nullable java.lang.String _val150;
          for (int _i151 = 0; _i151 < _map148.size; ++_i151)
          {
            _key149 = iprot.readString();
            _val150 = iprot.readString();
            struct.configuration.put(_key149, _val150);
          }
        }
        struct.setConfigurationIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

