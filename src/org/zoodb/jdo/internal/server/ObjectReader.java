/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.server;


import org.zoodb.jdo.api.impl.DBStatistics;
import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.PrimLongMapLI;

/**
 * This class serves as a mediator between the serializer and the file access class.
 * 
 * @author Tilmann Zäschke
 */
public class ObjectReader implements SerialInput {

	private final SerialInput in;
	
	public ObjectReader(StorageChannel file) {
		this.in = file.getReader(true);
	}

	public ObjectReader(SerialInput in) {
		this.in = in;
	}

    @Override
    public int readInt() {
        return in.readInt();
    }

    @Override
    public long readLong() {
        return in.readLong();
    }

    @Override
    public boolean readBoolean() {
        return in.readBoolean();
    }

    @Override
    public byte readByte() {
        return in.readByte();
    }

    @Override
    public char readChar() {
        return in.readChar();
    }

    @Override
    public double readDouble() {
        return in.readDouble();
    }

    @Override
    public float readFloat() {
        return in.readFloat();
    }

    @Override
    public short readShort() {
        return in.readShort();
    }

    @Override
    public void readFully(byte[] array) {
    	in.readFully(array);
    }

    @Override
    public String readString() {
        return in.readString();
    }

    @Override
    public void skipRead(int nBytes) {
    	in.skipRead(nBytes);
    }

    @Override
    public void seekPosAP(long pageAndOffs) {
        in.seekPosAP(pageAndOffs);
    }

	@Override
    public void seekPage(int page, int offs) {
        in.seekPage(page, offs);
    }

    public long startReading(int page, int offs) {
        in.seekPage(page, offs);
        if (DBStatistics.isEnabled()) {
        	statNRead++;
        	statNReadUnique.put(page, null);
        }
       return in.readHeaderClassOID();
    }
    
	private static final PrimLongMapLI<Object> statNReadUnique = new PrimLongMapLI<Object>();
	private static int statNRead = 0; 

	//@Override
	public static final int statsGetReadCount() {
		DatabaseLogger.debugPrintln(1, "WARNING: Using static read counter");
		return statNRead;
	}

	//@Override
	public static final int statsGetReadCountUnique() {
		DatabaseLogger.debugPrintln(1, "WARNING: Using static read counter");
		int ret = statNReadUnique.size();
		statNReadUnique.clear();
		return ret;
	}

	@Override
	public long readHeaderClassOID() {
		return in.readHeaderClassOID();
	}

	
	//    @Override
//    public String toString() {
//    	return "pos=" + file.getPage() + "/" + file.getOffset();
//    }
}
