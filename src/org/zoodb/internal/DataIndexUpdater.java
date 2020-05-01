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
package org.zoodb.internal;

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.Pair;


/**
 * This class provides a method to backup indexed fields for later removal from the according
 * field index. 
 *
 * @author Tilmann Zaeschke
 */
public final class DataIndexUpdater {

	private ZooFieldDef[] indFields;
	
	public DataIndexUpdater(ZooClassDef def) {
		refreshWithSchema(def);
	}
	
	/**
	 * TODO move this whole class into ZooClassDef? It seems bad that we store this information
	 * twice!
	 *  
	 * @param def Class definition
	 */
	public void refreshWithSchema(ZooClassDef def) {
		ArrayList<ZooFieldDef> pfl = new ArrayList<ZooFieldDef>();
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.isIndexed()) {
				pfl.add(f);
			}
		}
		indFields = pfl.toArray(new ZooFieldDef[pfl.size()]);
	}
	
	
    public final Pair<long[], Object[]> getBackup(ZooPC co) {
    	if (co.getClass() == GenericObject.class) {
    		GenericObject go = (GenericObject) co;
    		return getBackup(go, go.getRawFields());
    	}
    	if (indFields.length == 0) {
    		return null;
    	}
        try {
        	Pair<long[], Object[]> ret = new Pair<long[], Object[]>(
        			new long[indFields.length], new Object[indFields.length]);
            //set primitive fields
            for (int i = 0; i < indFields.length; i++) {
            	ZooFieldDef fd = indFields[i];
                Field f = fd.getJavaField();
                PRIMITIVE p = fd.getPrimitiveType();
                if (p != null) {
                	ret.getA()[i] = SerializerTools.primitiveFieldToLong(co, f, p);
                } else if (fd.isPersistentType()){
                	ZooPC pc = (ZooPC)f.get(co);
                	ret.getA()[i] = BitTools.toSortableLong(pc);
                	ret.getB()[i] = pc;
                } else {
                	//must be String
                	String str = (String)f.get(co);
                	ret.getA()[i] = BitTools.toSortableLong(str);
                	ret.getB()[i] = str;
                }
            }
            return ret;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    public final Pair<long[], Object[]> getBackup(GenericObject co, Object[] raw) {
    	if (indFields.length == 0) {
    		return null;
    	}
    	Pair<long[], Object[]> ret = new Pair<long[], Object[]>(
    			new long[indFields.length], new Object[indFields.length]);
    	//set primitive fields
    	for (int i = 0; i < indFields.length; i++) {
    		ZooFieldDef fd = indFields[i];
    		PRIMITIVE p = fd.getPrimitiveType();
    		if (p != null) {
    			ret.getA()[i] = SerializerTools.primitiveToLong(raw[fd.getFieldPos()], p);
            } else if (fd.isPersistentType()){
            	Object oid = raw[fd.getFieldPos()];
            	ret.getA()[i] = oid == null ? BitTools.NULL : (long)oid;
            	ret.getB()[i] = co.getField(fd);
    		} else {
    			//must be String (already hashed)
    			ret.getA()[i] = (Long)raw[fd.getFieldPos()];
    			ret.getB()[i] = co.getField(fd);
    		}
    	}
    	return ret;
    }

	public boolean isIndexed() {
		return indFields.length != 0;
	}
    
}
