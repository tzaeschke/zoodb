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
package org.zoodb.internal.model1p;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.DataSerializer;
import org.zoodb.internal.DataSink;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.SerializerTools;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.ObjectWriter;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.server.index.AbstractPagedIndex.LongLongIndex;
import org.zoodb.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;


/**
 * A data sink serializes objects of a given class. It can be backed either by a file- or
 * in-memory-storage, or in future by a network channel through which data is sent to a server.
 * 
 * Each sink handles objects of one class only. Therefore sinks can be associated with 
 * ZooClassDefs and PCContext instances.
 * 
 * TODO
 * get the schema indices only once, then update them in case schema/indices evolve.
 * 
 * @author ztilmann
 */
public class DataSink1P implements DataSink {

    private static final int BUFFER_SIZE = 1000;

    private final Node1P node;
    private final ZooClassDef cls;
    private final DataSerializer ds;
    private final ObjectWriter ow;
    private final ZooPCImpl[] buffer = new ZooPCImpl[BUFFER_SIZE];
    private int bufferCnt = 0;
    private final GenericObject[] bufferGO = new GenericObject[BUFFER_SIZE];
    private int bufferGOCnt = 0;
    private boolean isStarted = false;
    private final ArrayList<Pair>[] fieldUpdateBuffer;

    private static class Pair {
    	private final long oid;
    	private final long value;
    	public Pair(long oid, long value) {
    		this.oid = oid;
    		this.value = value;
		}
    }
    
    @SuppressWarnings("unchecked")
	public DataSink1P(Node1P node, AbstractCache cache, ZooClassDef cls, ObjectWriter out) {
        this.node = node;
        this.cls = cls;
        this.fieldUpdateBuffer = new ArrayList[cls.getAllFields().length];
        this.ds = new DataSerializer(out, cache, node);
        this.ow = out;
    }

    private void preWrite() {
        if (!isStarted) {
            ow.newPage();
            isStarted = true;
        }
    }

    /* (non-Javadoc)
     * @see org.zoodb.internal.model1p.DataSink#write(org.zoodb.api.impl.ZooPCImpl)
     */
    @Override
    public void write(ZooPCImpl obj) {
        preWrite();

        //write object
        ds.writeObject(obj, cls);

        //updated index
        //This is buffered to reduce look-ups to find field indices.
        buffer[bufferCnt++] = obj;
        if (bufferCnt == BUFFER_SIZE) {
            flushBuffer();
        }
    }

    @Override
    public void writeGeneric(GenericObject obj) {
        preWrite();

        //write object
        ds.writeObject(obj, cls);

        //updated index
        //This is buffered to reduce look-ups to find field indices.
        bufferGO[bufferGOCnt++] = obj;
        if (bufferGOCnt == BUFFER_SIZE) {
            flushBuffer();
        }
    }

    @Override
    public void reset() {
        if (isStarted) {
            ow.flush();  //TODO reset?
            //To avoid memory leaks...
            Arrays.fill(buffer, null);
            bufferCnt = 0;
            Arrays.fill(fieldUpdateBuffer, null);
            if (bufferGOCnt > 0) {
                Arrays.fill(bufferGO, null);
                bufferGOCnt = 0;
            }
            isStarted = false;
        }
    }
    
    /* (non-Javadoc)
     * @see org.zoodb.internal.model1p.DataSink#flush()
     */
    @Override
    public void flush() {
        if (isStarted) {
            flushBuffer();
            ow.flush();
            //To avoid memory leaks...
            Arrays.fill(buffer, null);
            isStarted = false;
        }
    }

    private void flushBuffer() {
        updateFieldIndices();
        bufferCnt = 0;
        if (bufferGOCnt > 0) {
	        updateFieldIndicesGO();
	        bufferGOCnt = 0;
        }

        for (int i = 0; i < fieldUpdateBuffer.length; i++) {
        	ArrayList<Pair> a = fieldUpdateBuffer[i];
        	if (a != null) {
            	ZooFieldDef field = cls.getAllFields()[i];
                if (!field.isIndexed()) {
                    continue;
                }
                SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType()); 
                LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
        		for (Pair p: a) {
        			//This should now work, all objects have been removed
        			//Refreshing is also not an issue, we already have the index-value
                	if (!fieldInd.insertLongIfNotSet(p.value, p.oid)) {
                		throw DBLogger.newUser("Unique index clash by value of field " 
                				+ field.getName() + "=" + p.value +  " of object "
                				+ Util.oidToString(p.oid));
                	}
        		}
        		fieldUpdateBuffer[i] = null;
        	}
        }
    }


    private void updateFieldIndices() {
        final ZooPCImpl[] buffer = this.buffer;
        final int bufferCnt = this.bufferCnt;

        //update field indices
        //We hook into the makeDirty call to store the previous value of the field such that we 
        //can remove it efficiently from the index.
        //Or is there another way, maybe by updating an (or the) index?
        int iInd = -1;
        int iField = -1;
        for (ZooFieldDef field: cls.getAllFields()) {
            iField++;
            if (!field.isIndexed()) {
                continue;
            }
            iInd++;

            //TODO?
            //For now we define that an index is shared by all classes and sub-classes that have
            //a matching field. So there is only one index which is defined in the top-most class
            SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType()); 
            LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
            try {
                Field jField = field.getJavaField();
                for (int i = 0; i < bufferCnt; i++) {
                    ZooPCImpl co = buffer[i];
                    if (!co.jdoZooIsNew()) {
                        //TODO It is bad that we update ALL indices here, even if the value didn't
                        //change... -> Field-wise dirty!
                        long l = co.jdoZooGetBackup()[iInd];
                        fieldInd.removeLong(l, co.jdoZooGetOid());
                    }
                    final long l;
                    if (field.isString()) {
                        String str = (String)jField.get(co);
                        l = BitTools.toSortableLong(str);
                    } else {
                    	l = SerializerTools.primitiveFieldToLong(co, jField, field.getPrimitiveType());
                    }
                    if (field.isIndexUnique()) {
                    	if (!fieldInd.insertLongIfNotSet(l, co.jdoZooGetOid())) {
                    		if (fieldUpdateBuffer[iField] == null) {
                    			fieldUpdateBuffer[iField] = new ArrayList<Pair>();
                    		}
                    		fieldUpdateBuffer[iField].add(new Pair(co.jdoZooGetOid(), l));
                    	}
                    } else {
                    	fieldInd.insertLong(l, co.jdoZooGetOid());
                    }
                }
            } catch (SecurityException e) {
                throw DBLogger.newFatal("Error accessing field: " + field.getName(), e);
            } catch (IllegalArgumentException e) {
                throw DBLogger.newFatal("Error accessing field: " + field.getName(), e);
            } catch (IllegalAccessException e) {
                throw DBLogger.newFatal("Error accessing field: " + field.getName(), e);
            }
        }
    }

    private void updateFieldIndicesGO() {
        final GenericObject[] buffer = this.bufferGO;
        final int bufferCnt = this.bufferGOCnt;

        //update field indices
        //We hook into the makeDirty call to store the previous value of the field such that we 
        //can remove it efficiently from the index.
        //Or is there another way, maybe by updating an (or the) index?
        int iInd = -1;
        int iField = -1;
        for (ZooFieldDef field: cls.getAllFields()) {
            iField++;
            if (!field.isIndexed()) {
                continue;
            }
            iInd++;

            //TODO?
            //For now we define that an index is shared by all classes and sub-classes that have
            //a matching field. So there is only one index which is defined in the top-most class
            SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType()); 
            LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
            try {
                for (int i = 0; i < bufferCnt; i++) {
                    GenericObject co = buffer[i];
                    if (!co.isNew()) {
                        //TODO It is bad that we update ALL indices here, even if the value didn't
                        //change... -> Field-wise dirty!
                        long l = co.jdoZooGetBackup()[iInd];
                        fieldInd.removeLong(l, co.getOid());
                    }
                    final long l;
                    if (field.isString()) {
                        l = (Long)co.getFieldRaw(iField);
                    } else {
                    	Object primO = co.getFieldRaw(iField);
                    	l = SerializerTools.primitiveToLong(primO, field.getPrimitiveType());
                    }
                    if (field.isIndexUnique()) {
                    	if (!fieldInd.insertLongIfNotSet(l, co.getOid())) {
                    		if (fieldUpdateBuffer[iField] == null) {
                    			fieldUpdateBuffer[iField] = new ArrayList<Pair>();
                    		}
                    		fieldUpdateBuffer[iField].add(new Pair(co.getOid(), l));
                    	}
                    } else {
                    	fieldInd.insertLong(l, co.getOid());
                    }
                }
            } catch (IllegalArgumentException e) {
                throw DBLogger.newFatal("Error accessing field: " + field.getName(), e);
            }
        }
    }
}
