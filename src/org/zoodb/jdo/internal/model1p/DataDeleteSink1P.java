/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
import javax.jdo.JDOObjectNotFoundException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.DataDeSerializerNoClass;
import org.zoodb.jdo.internal.DataDeleteSink;
import org.zoodb.jdo.internal.GenericObject;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.jdo.internal.util.Util;


/**
 * A data sink deletes objects of a given class. It can be backed either by a file- or
 * in-memory-storage, or in future by a network channel through which data is sent to a server.
 * 
 * Each sink handles objects of one class only. Therefore sinks can be associated with 
 * ZooClassDefs and PCContext instances.
 * 
 * @author ztilmann
 */
public class DataDeleteSink1P implements DataDeleteSink {

    private static final int BUFFER_SIZE = 1000;

    private final Node1P node;
    private final ZooClassDef cls;
    private final PagedOidIndex oidIndex;
    private SchemaIndexEntry sie;
    private final ZooPCImpl[] buffer = new ZooPCImpl[BUFFER_SIZE];
    private int bufferCnt = 0;
    private boolean isStarted = false;

    public DataDeleteSink1P(Node1P node, AbstractCache cache, ZooClassDef cls,
            PagedOidIndex oidIndex) {
        this.node = node;
        this.cls = cls;
        this.oidIndex = oidIndex;
        this.sie = node.getSchemaIE(cls);
    }

    /* (non-Javadoc)
     * @see org.zoodb.jdo.internal.model1p.DataSink#write(org.zoodb.api.impl.ZooPCImpl)
     */
    @Override
    public void delete(ZooPCImpl obj) {
        if (!isStarted) {
            this.sie = node.getSchemaIE(cls);
            isStarted = true;
        }

        //updated index
        //This is buffered to reduce look-ups to find field indices.
        buffer[bufferCnt++] = obj;
        if (bufferCnt == BUFFER_SIZE) {
            flushBuffer();
        }
    }

    @Override
    public void deleteGeneric(GenericObject obj) {
        if (!isStarted) {
            this.sie = node.getSchemaIE(cls);
            isStarted = true;
        }

        //updated index
        //TODO use buffer?!?!
        PagedPosIndex ois = sie.getObjectIndexLatestSchemaVersion();
        delete(obj.getOid(), ois);
//        //This is buffered to reduce look-ups to find field indices.
//        buffer[bufferCnt++] = obj;
//        if (bufferCnt == BUFFER_SIZE) {
//            flushBuffer();
//        }
    }

    @Override
    public void reset() {
        if (isStarted) {
            Arrays.fill(buffer, null);
            bufferCnt = 0;
            isStarted = false;
        }
    }

    /* (non-Javadoc)
     * @see org.zoodb.jdo.internal.model1p.DataSink#flush()
     */
    @Override
    public void flush() {
        if (isStarted) {
            flushBuffer();
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
        
 
        //remove field index entries
        int iInd = -1;
        for (ZooFieldDef field: cls.getAllFields()) {
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
                    //new and clean objects do not have a backup
                    if (!co.jdoZooIsNew()) {
                        //this can be null for objects that get deleted.
                        //These are still dirty, because of the deletion
                        if (co.jdoZooGetBackup()!=null) {
                            //TODO It is bad that we update ALL indices here, even if the value didn't
                            //change... -> Field-wise dirty!
                            long l = co.jdoZooGetBackup()[iInd];
                            fieldInd.removeLong(l, co.jdoZooGetOid());
                            continue;
                        }
                    }
                    if (field.isString()) {
                        if (co.zooIsHollow()) {
                        	//We need to activate it to get the values!
                        	//But only for String, the primitives should be fine.
                        	co.jdoZooGetContext().getNode().refreshObject(co);
                        }
                    	String str = (String)jField.get(co);
                        long l = (str != null ? 
                                BitTools.toSortableLong(str) : DataDeSerializerNoClass.NULL);
                        fieldInd.removeLong(l, co.jdoZooGetOid());
                    } else {
                        switch (field.getPrimitiveType()) {
                        case BOOLEAN: 
                            fieldInd.removeLong(jField.getBoolean(co) ? 1 : 0, co.jdoZooGetOid());
                            break;
                        case BYTE: 
                            fieldInd.removeLong(jField.getByte(co), co.jdoZooGetOid());
                            break;
                        case CHAR: 
                            fieldInd.removeLong(jField.getChar(co), co.jdoZooGetOid());
                            break;
                        case DOUBLE: 
                        	long ld = BitTools.toSortableLong(jField.getDouble(co));
                            fieldInd.removeLong(ld, co.jdoZooGetOid());
                            break;
                        case FLOAT:
                        	long lf = BitTools.toSortableLong(jField.getFloat(co));
                            fieldInd.removeLong(lf, co.jdoZooGetOid());
                            break;
                        case INT: 
                            fieldInd.removeLong(jField.getInt(co), co.jdoZooGetOid());
                            break;
                        case LONG: 
                            fieldInd.removeLong(jField.getLong(co), co.jdoZooGetOid());
                            break;
                        case SHORT: 
                            fieldInd.removeLong(jField.getShort(co), co.jdoZooGetOid());
                            break;

                        default:
                            throw new IllegalArgumentException("type = " + field.getPrimitiveType());
                        }
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
        
        //now delete the object
        PagedPosIndex ois = sie.getObjectIndexLatestSchemaVersion();
        for (int i = 0; i < bufferCnt; i++) {
            long oid = buffer[i].jdoZooGetOid();
            delete(oid, ois);
        }

    }
    
    private void delete(long oid, PagedPosIndex ois) {
    	long pos = oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
    	if (pos == -1) {
    		throw new JDOObjectNotFoundException("Object not found: " + Util.oidToString(oid));
    	}

    	//update class index and
    	//tell the FSM about the free page (if we have one)
    	//prevPos.getValue() returns > 0, so the loop is performed at least once.
    	do {
    		//remove and report to FSM if applicable
    		long nextPos = ois.removePosLongAndCheck(pos);
    		//use mark for secondary pages
    		nextPos = nextPos | PagedPosIndex.MARK_SECONDARY;
    		pos = nextPos;
    	} while (pos != PagedPosIndex.MARK_SECONDARY);
    }
}