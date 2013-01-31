/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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

import java.util.ArrayList;
import java.util.Arrays;

import org.zoodb.jdo.internal.server.ObjectReader;

/**
 * Instances of this class represent persistent instances that can not be de-serialised into
 * Java classes, because according Java classes do not exist. This can for example occur during
 * schema evolution. 
 * 
 * Generating required classes is not always possible, because there may be a class with the 
 * required name but with a wrong set of attributes.
 * TODO a solution would be to generate temporary Java classes for de-serialisation, using
 * a temporary name such as Zoo__Generic__[ClassName].
 * 
 * Since we have to insert/delete attributes, we need to split them up 
 * (otherwise, just mark them as deleted? how about inserted???).
 * 
 * Uses:
 * - One aim is to return a bitstream with the correct schema (transport to client)
 * - Store locally with updated schema -> requires updated bit-stream
 * - Alternatively, allow reading as if the schema was modified (through transparent mapping)???
 * 
 * What should the output be:
 * - A bitstream: Can be transferred to client (or server), can be directly stored or fed to
 *   DeSerialiser.
 * - A generic object that allows random access? This could also be achieved by using a bitstream
 *   and the NoClass(De)Serializer.
 * 
 * Output: BIT-STREAM!
 * TODO rename to SchemaMapper? Input: byte[]; output: evolved byte[]
 * 
 * However, byte[] is not ideal for random access (updates). How do we do updates? Updates 
 * require resizing (e.g. String). This could be done on-the-fly on a byte[] or afterwards
 * using an existing Serializer (?); not quite, an existing Serializer takes real objects as input,
 * not serialised ones (important for SCOs?!?!?).
 * 
 *  
 * How is deserialisation different?
 * - Assigning values:
 *   - values are assigned to an Object[] instead of Field instances. This affects only 
 *     readPrimitive() and deserializeFields1()/2().
 * - References:
 *   - FCOs (hollowToObject()) returns a Long instead of an PCImpl
 *   - SCOs return a byte[] instead of Object.
 *   
 * --> CHECK Looks like we could integrate thus into the existing deserializer... 
 *  
 * This is clear:
 * - Input: Bit-Stream, SchemaDefinition, SchemaMapping
 * - Capability: Mapping (on-the-fly or permanent evolution) of schemata.
 * - Output: ?
 * 
 * - Input #2: Updates to fields
 * - Output #2: Individual fields
 * - I/O #2: Could be primitives, but also byte[] for SCOs!!!!
 * 
 * @author Tilmann Zaschke
 */
public class GenericObject {

	private ZooClassDef def;
	private final long oid;
	//TODO keep in single ba[]?!?!?
	private Object[] fixedValues;
	private Object[] variableValues;
	
	
	public GenericObject(ZooClassDef def, long oid) {
		this.def = def;
		this.oid = oid;
		fixedValues = new Object[def.getAllFields().length];
		variableValues = new Object[def.getAllFields().length];
	}
	
	public void read(ObjectReader in) {
		int i = 0;
		//read fixed part
		for (ZooFieldDef f: def.getAllFields()) {
			int len = f.getLength();
			byte[] ba = new byte[len];
			in.readFully(ba);
			fixedValues[i] = ba;
			i++;
		}
		//read variable length
		for (ZooFieldDef f: def.getAllFields()) {
			if (!f.isFixedSize()) {
				if (f.isString()) {
					variableValues[i] = in.readString();
				} else if (f.isArray()) {
					throw new UnsupportedOperationException();
				} else { //SCO?!!?
					throw new UnsupportedOperationException();
				}
				
				//TODO or we store only positions, which allows easy insertion/removal of bytes...
			}
			i++;
		}
		
	}

	public void setFieldRAW(int i, Object deObj) {
		fixedValues[i] = deObj;
		//TODO remove
		if (!def.getAllFields()[i].isFixedSize()) {
			throw new IllegalArgumentException();
		}
	}

	public void setFieldRawSCO(int i, Object deObj) {
		variableValues[i] = deObj;
		if (def.getAllFields()[i].isFixedSize() || def.getAllFields()[i].isPrimitiveType()) {
			throw new IllegalArgumentException();
		}
	}

	public ZooClassDef evolve() {
		//TODO this is horrible!!!
		ArrayList<Object> fV = new ArrayList<Object>(Arrays.asList(fixedValues));
		ArrayList<Object> vV = new ArrayList<Object>(Arrays.asList(variableValues));
		
		//TODO resize only once to correct size
		for (PersistentSchemaOperation op: def.getNextVersion().getEvolutionOps()) {
			if (op.isAddOp()) {
				fV.add(op.getFieldId(), op.getInitialValue());
				vV.add(op.getFieldId(), null);
			} else {
				fV.remove(op.getInitialValue());
				vV.remove(op.getInitialValue());
			}
		}
		fixedValues = fV.toArray(fixedValues);
		variableValues = vV.toArray(variableValues);
		def = def.getNextVersion();
		return def;
	}

	public ObjectReader toStream() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}
	
}
