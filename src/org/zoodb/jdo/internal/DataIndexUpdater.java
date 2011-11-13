/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.zoodb.jdo.internal.SerializerTools.PRIMITIVE;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.spi.PersistenceCapableImpl;


/**
 * This class provides a method to backup indexed fields for later removal from the according
 * field index. 
 * 
 * @author Tilmann Zaeschke
 */
public final class DataIndexUpdater {

	private final ZooFieldDef[] indFields;
	
	public DataIndexUpdater(ZooClassDef def) {
		ArrayList<ZooFieldDef> pfl = new ArrayList<ZooFieldDef>();
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.isIndexed()) {
				pfl.add(f);
			}
		}
		indFields = pfl.toArray(new ZooFieldDef[pfl.size()]);
	}
	
	
    public final long[] getBackup(PersistenceCapableImpl co) {
    	if (indFields.length == 0) {
    		return null;
    	}
        try {
        	long[] la = new long[indFields.length];
            //set primitive fields
            for (int i = 0; i < indFields.length; i++) {
            	ZooFieldDef fd = indFields[i];
                Field f = fd.getJavaField();
                PRIMITIVE p = fd.getPrimitiveType();
                if (p != null) {
                	la[i] = readPrimitive(co, f, p);
                } else {
                	//must be String
                	String str = (String)f.get(co);
                	if (str != null) {
                		la[i] = BitTools.toSortableLong(str);
                	} else {
                		la[i] = DataDeSerializerNoClass.NULL;
                	}
                }
            }
            return la;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final long readPrimitive(Object parent, Field field, PRIMITIVE prim) 
    throws IllegalArgumentException, IllegalAccessException {
        switch (prim) {
        case BOOLEAN: return field.getBoolean(parent) ? 1L : 0L;
        case BYTE: return field.getByte(parent);
        case CHAR: return field.getChar(parent);
        case DOUBLE: return BitTools.toSortableLong(field.getDouble(parent));
        case FLOAT: return BitTools.toSortableLong(field.getFloat(parent));
        case INT: return field.getInt(parent);
        case LONG: return field.getLong(parent);
        case SHORT: return field.getShort(parent);
        default:
            throw new UnsupportedOperationException(prim.toString());
        }
    }
}
