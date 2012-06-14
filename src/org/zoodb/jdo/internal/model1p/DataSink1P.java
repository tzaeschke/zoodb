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
package org.zoodb.jdo.internal.model1p;

import java.lang.reflect.Field;
import java.util.Arrays;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.DataDeSerializerNoClass;
import org.zoodb.jdo.internal.DataSerializer;
import org.zoodb.jdo.internal.DataSink;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.ObjectWriter;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;


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
    private boolean isStarted = false;
    
    private PagedPosIndex posIndex;
    
    public DataSink1P(Node1P node, AbstractCache cache, ZooClassDef cls, ObjectWriter out) {
        this.node = node;
        this.cls = cls;
        this.ds = new DataSerializer(out, cache, node);
        this.ow = out;
    }

    private void preWrite() {
        if (!isStarted) {
            this.posIndex = node.getSchemaIE(cls.getOid()).getObjectIndex();
            ow.newPage(posIndex, cls.getOid());
            isStarted = true;
        }
    }
    
    /* (non-Javadoc)
     * @see org.zoodb.jdo.internal.model1p.DataSink#write(org.zoodb.api.impl.ZooPCImpl)
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
    
    /* (non-Javadoc)
     * @see org.zoodb.jdo.internal.model1p.DataSink#flush()
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
    }
    
    
    private void updateFieldIndices() {
        final ZooPCImpl[] buffer = this.buffer;
        final int bufferCnt = this.bufferCnt;

        //update field indices
        //We hook into the makeDirty call to store the previous value of the field such that we 
        //can remove it efficiently from the index.
        //Or is there another way, maybe by updating an (or the) index?
        int iInd = -1;
        for (ZooFieldDef field: cls.getAllFields()) {
            if (!field.isIndexed()) {
                continue;
            }
            iInd++;
            
            //TODO?
            //For now we define that an index is shared by all classes and sub-classes that have
            //a matching field. So there is only one index which is defined in the top-most class
            SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType().getOid()); 
            LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
            try {
                Field jField = field.getJavaField();
                if (field.isString()) {
                    for (int i = 0; i < bufferCnt; i++) {
                        ZooPCImpl co = buffer[i];
                        if (!co.jdoZooIsNew()) {
                            long l = co.jdoZooGetBackup()[iInd];
                            fieldInd.removeLong(l, co.jdoZooGetOid());
                        }
                        String str = (String)jField.get(co);
                        if (str != null) {
                            long l = BitTools.toSortableLong(str);
                            fieldInd.insertLong(l, co.jdoZooGetOid());
                        } else {
                            fieldInd.insertLong(DataDeSerializerNoClass.NULL, co.jdoZooGetOid());
                        }
                    }
                } else {
                    switch (field.getPrimitiveType()) {
                    case BOOLEAN: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getBoolean(co) ? 1 : 0, co.jdoZooGetOid());
                        }
                        break;
                    case BYTE: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getByte(co), co.jdoZooGetOid());
                        }
                        break;
                    case CHAR: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getChar(co), co.jdoZooGetOid());
                        }
                        break;
                    case DOUBLE: 
                        System.out.println("STUB DiskAccessOneFile.writeObjects(DOUBLE)");
                        //TODO
    //                  for (CachedObject co: cachedObjects) {
                            //fieldInd.insertLong(jField.getDouble(co.obj), co.oid);
    //                  }
                        break;
                    case FLOAT:
                        //TODO
                        System.out.println("STUB DiskAccessOneFile.writeObjects(FLOAT)");
    //                  for (CachedObject co: cachedObjects) {
    //                      fieldInd.insertLong(jField.getFloat(co.obj), co.oid);
    //                  }
                        break;
                    case INT: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getInt(co), co.jdoZooGetOid());
                        }
                        break;
                    case LONG: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getLong(co), co.jdoZooGetOid());
                        }
                        break;
                    case SHORT: 
                        for (int i = 0; i < bufferCnt; i++) {
                            ZooPCImpl co = buffer[i];
                            if (!co.jdoZooIsNew()) {
                                long l = co.jdoZooGetBackup()[iInd];
                                fieldInd.removeLong(l, co.jdoZooGetOid());
                            }
                            fieldInd.insertLong(jField.getShort(co), co.jdoZooGetOid());
                        }
                        break;
                        
                    default:
                        throw new IllegalArgumentException("type = " + field.getPrimitiveType());
                    }
                }
            } catch (SecurityException e) {
                throw new JDOFatalDataStoreException(
                        "Error accessing field: " + field.getName(), e);
            } catch (IllegalArgumentException e) {
                throw new JDOFatalDataStoreException(
                        "Error accessing field: " + field.getName(), e);
            } catch (IllegalAccessException e) {
                throw new JDOFatalDataStoreException(
                        "Error accessing field: " + field.getName(), e);
            }
        }
    }
}
