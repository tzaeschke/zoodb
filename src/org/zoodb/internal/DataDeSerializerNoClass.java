/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal;

import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.Util;


/**
 * This class allows reading individual attributes of a serialized class. 
 * 
 * @author Tilmann Zaeschke
 */
public class DataDeSerializerNoClass {

    private final StorageChannelInput in;
    private long oid;
    private long clsOid;
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * persistent.
     */
    public DataDeSerializerNoClass(StorageChannelInput in) {
        this.in = in;
//        //Read OID
//    	oid = in.readLong();
//        //read class info:
//    	clsOid = in.readLongAtOffset(0);
    }
        
    public void seekPos(long pos) {
    	in.seekPosAP(DATA_TYPE.DATA, pos);
    }
    
    private int readHeader(ZooClassDef clsDef) {
    	return readHeader(clsDef, false);
    }
    	
    private int readHeader(ZooClassDef clsDef, boolean allowSchemaMismatch) {
        //Read object header. 
        //Read OID
    	oid = in.readLong();
        //read class info:
    	clsOid = in.getHeaderClassOID();
    	if (!allowSchemaMismatch && clsOid != clsDef.getOid()) {
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
    
    public long getClassOid() {
    	//readHeader(clsDef)
        //Read OID
    	oid = in.readLong();
        //read class info:
    	clsOid = in.getHeaderClassOID();
    	return clsOid;
    }
    
    public long getOid() {
    	//The oid position is independent of the schema version
    	readHeader(null, true);
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
    		//long has different sorting order than double!
    		return BitTools.toSortableLong(in.readDouble());
    	}
    	case FLOAT: {
    		//long has different sorting order than float!
    		return BitTools.toSortableLong(in.readFloat());
    	}
    	case INT: return in.readInt();
    	case LONG: return in.readLong();
    	case SHORT: return in.readShort();
    	default: 
    		throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
    	}
	}
	
//	public Long getAttrAsLongObject(ZooClassDef clsDef, ZooFieldDef field) {
//    	int skip = readHeader(clsDef);
//    	skip += field.getOffset();
//    	in.skipRead(skip);
//    	if (in.readByte() == -1) {
//    		return null;
//    	}
//		switch (field.getJdoType()) {
//		case DATE: return in.readLong();
//		case STRING: return in.readLong();
//		case REFERENCE: return in.readLong();
//		default: 
//			throw new IllegalArgumentException(field.getJdoType() + " " + field.getName());
//		}
//	}

	public long getAttrAsLongObjectNotNull(ZooClassDef clsDef, ZooFieldDef field) {
    	int skip = readHeader(clsDef);
    	skip += field.getOffset();
    	in.skipRead(skip);
    	if (in.readByte() == -1) {
    		return BitTools.NULL;
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
