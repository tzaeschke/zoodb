package org.zoodb.jdo.internal;


/**
 * This class allows reading individual attributes of a serialized class. 
 * 
 * @author Tilmann Zaeschke
 */
public class DataDeSerializerNoClass {

    private final SerialInput _in;
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * persistent.
     */
    public DataDeSerializerNoClass(SerialInput in) {
        _in = in;
    }
        
    private int readHeader(ZooClassDef clsDef) {
        //Read object header. 
        //read class info:
    	long clsOid = _in.readLong();
    	if (clsOid != clsDef.getOid()) {
    		throw new UnsupportedOperationException("Schema evolution not yet supported.");
    	}
    	//ZooClassDef clsDef = _cache.getSchema(clsOid);
        //Read LOID
        
    	//long oid = _in.readLong();
    	return -8;  //we already read 8 bytes, so we don't need to skip them anymore
        
    	//TODO check cache? We could use reflection to extract data from there.
        //On the client that should definitely be done. On the server, we don't have a cache with
        //instantiated object, only pages or possibly byte arrays.
        
    }
    
    public long getAttrLong(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readLong();
    }

    public int getAttrInt(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readInt();
    }

	public byte getAttrByte(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readByte();
	}

	public short getAttrShort(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readShort();
	}

	public double getAttrDouble(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readDouble();
	}

	public float getAttrFloat(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readFloat();
	}

	public char getAttrChar(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readChar();
	}

	public boolean getAttrBool(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	return _in.readBoolean();
	}

	/**
     * @param clsDef
     * @param field
     * @return The magic number of the String or 'null' if the String is null.
     */
    public Long getStringMagic(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	if (_in.readByte() == -1) {
    		return null;
    	}
    	return getAttrLong(clsDef, field);
    }

	public long getAttrRefOid(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	if (_in.readByte() == -1) {
    		return OidBuffer.NULL_REF;
    	}
    	_in.readLong(); //schema
    	return _in.readLong();
	}

	public long getAttrAsLong(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	switch (field.getPrimitiveType()) {
    	case BOOLEAN: return _in.readBoolean() ? 1 : 0;
    	case BYTE: return _in.readByte();
    	case CHAR: return _in.readChar();
    	case DOUBLE: {
    		//TODO long has different sorting order than double!
    		System.out.println("STUB DataDeserializerNoClass.getAttrAsLong()");
    		return _in.readLong();
    		//return _in.readDouble();
    	}
    	case FLOAT: {
    		//TODO long has different sorting order than float!
    		System.out.println("STUB DataDeserializerNoClass.getAttrAsLong()");
    		return _in.readLong();
    		//return _in.readFloat();
    	}
    	case INT: return _in.readInt();
    	case LONG: return _in.readLong();
    	case SHORT: return _in.readShort();
    	default: 
    		throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
    	}
	}
	
	public Long getAttrAsLongObject(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	_in.skipRead(skip);
    	if (_in.readByte() == -1) {
    		return null;
    	}
		switch (field.getJdoType()) {
		case DATE: return _in.readLong();
		case STRING: return _in.readLong();
		case REFERENCE: return _in.readLong();
		default: 
			throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
		}
	}
}
