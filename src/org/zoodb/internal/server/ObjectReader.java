/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.server;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.internal.SerialInput;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.util.PrimLongSetZ;
import org.zoodb.tools.DBStatistics;

/**
 * This class serves as a mediator between the serializer and the file access class.
 * 
 * @author Tilmann Zaeschke
 */
public class ObjectReader implements SerialInput {

	public static final Logger LOGGER = LoggerFactory.getLogger(ObjectReader.class);

	private final SerialInput in;
	
	public ObjectReader(IOResourceProvider file) {
		this.in = file.createReader(true);
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

    public long startReading(int page, int offs) {
    	//TODO Hmm this is dirty...
        ((StorageChannelInput)in).seekPage(PAGE_TYPE.DATA, page, offs);
        if (DBStatistics.isEnabled()) {
        	statNRead++;
        	statNReadUnique.add(page);
        }
        return in.getHeaderClassOID();
    }
    
	private static final PrimLongSetZ statNReadUnique = new PrimLongSetZ();
	private static int statNRead = 0; 

	//@Override
	public static final int statsGetReadCount() {
		LOGGER.warn("WARNING: Using static read counter");
		return statNRead;
	}

	//@Override
	public static final int statsGetReadCountUnique() {
		LOGGER.warn("WARNING: Using static read counter");
		int ret = statNReadUnique.size();
		statNReadUnique.clear();
		return ret;
	}

	@Override
	public long getHeaderClassOID() {
		return in.getHeaderClassOID();
	}

	@Override
	public long getHeaderTimestamp() {
		return in.getHeaderTimestamp();
	}

	
	//    @Override
//    public String toString() {
//    	return "pos=" + file.getPage() + "/" + file.getOffset();
//    }
}
