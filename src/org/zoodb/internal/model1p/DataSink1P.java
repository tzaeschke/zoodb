/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
import java.util.Iterator;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataSerializer;
import org.zoodb.internal.DataSink;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.SerializerTools;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.ObjectWriter;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.server.index.LongLongIndex;
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
 * @author Tilmann Zaschke
 */
public class DataSink1P implements DataSink {

    private static final int BUFFER_SIZE = 1000;

    private final Node1P node;
    private final ZooClassDef cls;
    private final DataSerializer ds;
    private final ObjectWriter ow;
    private final ZooPC[] buffer = new ZooPC[BUFFER_SIZE];
    private int bufferCnt = 0;
    private final GenericObject[] bufferGO = new GenericObject[BUFFER_SIZE];
    private int bufferGOCnt = 0;
    private boolean isStarted = false;
    private final ArrayList<Pair>[] fieldUpdateBuffer;

    private static class Pair {
    	private final ZooPC pc;
    	private final long value;
    	public Pair(ZooPC pc, long value) {
    		this.pc = pc;
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
     * @see org.zoodb.internal.model1p.DataSink#write(org.zoodb.api.impl.ZooPC)
     */
    @Override
    public void write(ZooPC obj) {
        if (obj.getClass() == GenericObject.class) {
        	writeGeneric((GenericObject) obj);
        	return;
        }
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

        //Now perform all index updates that previously failed (because of unique index collisions).
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
        			if (field.isString()) {
        				//TODO this does not work for GOs... Actually, it might, because 
        				//GOs extend ZooPC...  for ZooPC see Test_091 -> Issue 55
        				String str = getString(p.pc, field);
        				//ignore 'null' ?!?!? Why? No reason, just a definition we make here...
        				if (str != null) {
	        				Iterator<ZooPC> it = 
	        						node.readObjectFromIndex(field, p.value, p.value, true);
	        				while (it.hasNext()) {
	        					ZooPC o2 = it.next();
	        					String s2 = getString(o2, field);
	        					if (str.equals(s2)) {
	                        		long oid2 = o2.jdoZooGetOid();
	                        		throw DBLogger.newUser("Unique index clash by value of field " 
	                        				+ field.getName() + "=" + p.value +  " of new object "
	                        				+ Util.oidToString(p.pc.jdoZooGetOid()) + " with "
	                        				+ Util.oidToString(oid2));
	        					}
	        				}
        				}
                    	fieldInd.insertLong(p.value, p.pc.jdoZooGetOid());
        			} else if (!fieldInd.insertLongIfNotSet(p.value, p.pc.jdoZooGetOid())) {
                		long oid2 = fieldInd.iterator(p.value, p.value).next().getValue();
                		throw DBLogger.newUser("Unique index clash by value of field " 
                				+ field.getName() + "=" + p.value +  " of new object "
                				+ Util.oidToString(p.pc.jdoZooGetOid()) + " with "
                				+ Util.oidToString(oid2));
                	}
        		}
        		fieldUpdateBuffer[i] = null;
        	}
        }
    }

    private String getString(ZooPC pc, ZooFieldDef field) {
		if (pc instanceof GenericObject) {
			GenericObject go = (GenericObject) pc;
			return (String) go.getField(field);
		} else { 
			try {
				return (String) field.getJavaField().get(pc);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
    }

    private void updateFieldIndices() {
        final ZooPC[] buffer = this.buffer;
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

            //For now we define that an index is shared by all classes and sub-classes that have
            //a matching field. So there is only one index which is defined in the top-most class
            SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType()); 
            LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
            try {
                Field jField = field.getJavaField();
                for (int i = 0; i < bufferCnt; i++) {
                    ZooPC co = buffer[i];
                    final long l;
                    String str = null;
                    if (field.isString()) {
                        str = (String)jField.get(co);
                        l = BitTools.toSortableLong(str);
                    } else {
                    	l = SerializerTools.primitiveFieldToLong(co, jField, field.getPrimitiveType());
                    }
                    if (!co.jdoZooIsNew()) {
                        long lOld = co.jdoZooGetBackup().getA()[iInd];
                        //Only update if value did not change
                        if (lOld == l) {
                        	if (field.isString()) {
                        		String str2 = (String) co.jdoZooGetBackup().getB()[iInd];
                        		if ((str==null && str2 == null) || str.equals(str2)) {
                                	//no update here...
                                	continue;
                        		}
                        	} else { 
	                        	//no update here...
	                        	continue;
                        	}
                        }
                        fieldInd.removeLong(lOld, co.jdoZooGetOid());
                    }
                    if (field.isIndexUnique()) {
                    	if (field.isString()) {
                    		//always buffer string updates, because verifying collisions is costly
                    		bufferIndexUpdate(iField, co, l);
                    	} else {
	                    	if (!fieldInd.insertLongIfNotSet(l, co.jdoZooGetOid())) {
	                        	bufferIndexUpdate(iField, co, l);
	                    	}
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

    private void bufferIndexUpdate(int iField, ZooPC pc, long l) {
   		if (fieldUpdateBuffer[iField] == null) {
			fieldUpdateBuffer[iField] = new ArrayList<Pair>();
		}
		fieldUpdateBuffer[iField].add(new Pair(pc, l));
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

            //For now we define that an index is shared by all classes and sub-classes that have
            //a matching field. So there is only one index which is defined in the top-most class
            SchemaIndexEntry schemaTop = node.getSchemaIE(field.getDeclaringType()); 
            LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
            try {
                for (int i = 0; i < bufferCnt; i++) {
                    GenericObject co = buffer[i];
                    final long l;
                    String str = null;
                    if (field.isString()) {
                        l = (Long)co.getFieldRaw(iField);
                        str = (String) co.getField(field); 
                    } else {
                    	Object primO = co.getFieldRaw(iField);
                    	l = SerializerTools.primitiveToLong(primO, field.getPrimitiveType());
                    }
                    if (!co.jdoZooIsNew()) {
                        long lOld = co.jdoZooGetBackup().getA()[iInd];
                        //Only update if value did not change
                        if (lOld == l) {
                           	if (field.isString()) {
                        		String str2 = (String) co.jdoZooGetBackup().getB()[iInd];
                        		if ((str==null && str2 == null) || str.equals(str2)) {
                                	//no update here...
                                	continue;
                        		}
                        	} else { 
	                        	//no update here...
	                        	continue;
                        	}
                        }
                        fieldInd.removeLong(lOld, co.getOid());
                    }
                    if (field.isIndexUnique()) {
                    	if (!fieldInd.insertLongIfNotSet(l, co.getOid())) {
                    		bufferIndexUpdate(iField, co, l);
                    	}
                    } else if (field.isString()) {
                    	//always buffer string updates, because verifying collisions is costly
                    	bufferIndexUpdate(iField, co, l);
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
