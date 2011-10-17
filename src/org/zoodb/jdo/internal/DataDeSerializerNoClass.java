package org.zoodb.jdo.internal;

import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.index.BitTools;


/**
 * This class allows reading individual attributes of a serialized class. 
 * 
 * @author Tilmann Zaeschke
 */
public class DataDeSerializerNoClass {

	//TODO store ZooCLassDef here?
    private final PageAccessFile in;
    private long oid;
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * persistent.
     */
    public DataDeSerializerNoClass(PageAccessFile in) {
        this.in = in;
    }
        
    public void seekPos(long pos) {
    	in.seekPos(pos, true);
    }
    
    private int readHeader(ZooClassDef clsDef) {
        //Read object header. 
        //Read OID
    	oid = in.readLong();
        //read class info:
    	long clsOid = in.readLongAtOffset(0);
    	if (clsOid != clsDef.getOid()) {
    		System.err.println();
    		throw new UnsupportedOperationException("Schema evolution not yet supported: " + 
    				clsDef.getClassName() + ": " + Util.oidToString(clsDef.getOid()) + " to " + 
    				Util.oidToString(clsOid));
    	}
        
    	//we already read 8 bytes, so we don't need to skip them anymore
        return -ZooFieldDef.OFS_INIITIAL;  
        
    	//TODO check cache? We could use reflection to extract data from there.
        //On the client that should definitely be done. On the server, we don't have a cache with
        //instantiated object, only pages or possibly byte arrays.
    }
    
    public long getOid(ZooClassDef clsDef) {
    	readHeader(clsDef);
    	return getLastOid();
    }
    
    public long getAttrLong(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readLong();
    }

    public int getAttrInt(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readInt();
    }

	public byte getAttrByte(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readByte();
	}

	public short getAttrShort(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readShort();
	}

	public double getAttrDouble(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readDouble();
	}

	public float getAttrFloat(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readFloat();
	}

	public char getAttrChar(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readChar();
	}

	public boolean getAttrBool(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	return in.readBoolean();
	}

	/**
     * @param clsDef
     * @param field
     * @return The magic number of the String or 'null' if the String is null.
     */
    public Long getStringMagic(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	if (in.readByte() == -1) {
    		return null;
    	}
    	return getAttrLong(clsDef, field);
    }

	public long getAttrRefOid(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	if (in.readByte() == -1) {
    		return OidBuffer.NULL_REF;
    	}
    	in.readLong(); //schema
    	return in.readLong();
	}

	public long getAttrAsLong(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	switch (field.getPrimitiveType()) {
    	case BOOLEAN: return in.readBoolean() ? 1 : 0;
    	case BYTE: return in.readByte();
    	case CHAR: return in.readChar();
    	case DOUBLE: {
    		//TODO long has different sorting order than double!
    		System.out.println("STUB DataDeserializerNoClass.getAttrAsLong()");
    		return BitTools.toSortableLong(in.readDouble());
    		//return _in.readDouble();
    	}
    	case FLOAT: {
    		//TODO long has different sorting order than float!
    		System.out.println("STUB DataDeserializerNoClass.getAttrAsLong()");
    		return BitTools.toSortableLong(in.readFloat());
    		//return _in.readFloat();
    	}
    	case INT: return in.readInt();
    	case LONG: return in.readLong();
    	case SHORT: return in.readShort();
    	default: 
    		throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
    	}
	}
	
	public Long getAttrAsLongObject(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	if (in.readByte() == -1) {
    		return null;
    	}
		switch (field.getJdoType()) {
		case DATE: return in.readLong();
		case STRING: return in.readLong();
		case REFERENCE: return in.readLong();
		default: 
			throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
		}
	}

	public long getLastOid() {
		return oid;
	}
}
