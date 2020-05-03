/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	public static int statsGetReadCount() {
		LOGGER.warn("WARNING: Using static read counter");
		return statNRead;
	}

	//@Override
	public static int statsGetReadCountUnique() {
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
