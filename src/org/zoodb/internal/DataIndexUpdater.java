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
	 * @param def
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
