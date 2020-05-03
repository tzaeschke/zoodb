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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.client.SchemaOperation;
import org.zoodb.internal.client.SchemaOperation.SchemaFieldDefine;
import org.zoodb.internal.client.SchemaOperation.SchemaFieldDelete;
import org.zoodb.internal.server.ObjectReader;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;
import org.zoodb.tools.internal.ObjectCache.GOProxy;

/**
 * Instances of this class represent persistent instances that cannot be de-serialised into
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
 * - Store locally with updated schema. This requires updated bit-stream
 * - Alternatively, allow reading as if the schema was modified (through transparent mapping)???
 * 
 * What should the output be:
 * - A bitstream: Can be transferred to client (or server), can be directly stored or fed to
 *   DeSerializer.
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
 * How is deserialization different?
 * - Assigning values:
 *   - values are assigned to an Object[] instead of Field instances. This affects only 
 *     readPrimitive() and deserializeFields1()/2().
 * - References:
 *   - FCOs (hollowToObject()) returns a Long instead of an PCImpl
 *   - SCOs return a byte[] instead of Object.
 *   
 * TODO CHECK: Looks like we could integrate thus into the existing deserializer... 
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
public class GenericObject extends ZooPC {

    private ZooClassDef defCurrent;
    private ZooClassDef defOriginal;

    //TODO keep in single ba[]?!?!?
	private Object[] fixedValues;
	private Object[] variableValues;
	
	private boolean isDbCollection = false; //== instanceof DBCollection
	private Object dbCollectionData;
	
	private ZooHandleImpl handle = null;
	
	private GenericObject(ObjectState state, ZooClassDef def, long oid, boolean isNew, 
			AbstractCache cache) {
		jdoZooInit(state, def.getProvidedContext(), oid);
		this.defCurrent = def;
		this.defOriginal = def;
		fixedValues = new Object[def.getAllFields().length];
		variableValues = new Object[def.getAllFields().length];
		cache.addGeneric(this);
		if (isNew) {
			jdoZooSetTimestamp(def.getProvidedContext().getSession().getTransactionId());
		}
	}

	public static GenericObject newInstance(ZooClassDef def, long oid, boolean isNewAndDirty,
			AbstractCache cache) {
		GenericObject go = new GenericObject(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL,
				def, oid, isNewAndDirty, cache);
		return go;
	}
	
	/**
	 * Creates new instances.
	 * @param def schema
	 * @return A new empty generic object.
	 */
	static GenericObject newEmptyInstance(ZooClassDef def, AbstractCache cache) {
		long oid = def.getProvidedContext().getNode().getOidBuffer().allocateOid();
		return newEmptyInstance(oid, def, cache);
	} 
	
	static GenericObject newEmptyInstance(long oid, ZooClassDef def, AbstractCache cache) {
		def.getProvidedContext().getNode().getOidBuffer().ensureValidity(oid);
		GenericObject go = new GenericObject(ObjectState.PERSISTENT_NEW, def, oid, true, cache);
	
		//initialise
		//We do not use default values here, because we are not evolving objects (is that a 
		//good reason?)
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.isPrimitiveType()) {
				Object x;
				switch (f.getPrimitiveType()) {
				case BOOLEAN: x = false; break;
				case BYTE: x = (byte)0; break;
				case CHAR: x = (char)0; break;
				case DOUBLE: x = (double)0; break;
				case FLOAT: x = (float)0; break;
				case INT: x = (int)0; break;
				case LONG: x = (long)0; break;
				case SHORT: x = (short)0; break;
				default: throw new IllegalStateException();
				}
				go.setField(f, x);
			} else if (f.isString()) {
				go.setField(f, null);
			}
		}
		
		return go;
	}
	
	public void setFieldRAW(int i, Object deObj) {
		fixedValues[i] = deObj;
	}

	public Object getFieldRaw(int i) {
		return fixedValues[i];
	}

	public void setFieldRawSCO(int i, Object deObj) {
		variableValues[i] = deObj;
		if (deObj instanceof String) {
			fixedValues[i] = BitTools.toSortableLong((String)deObj);
		}
	}

	public Object getFieldRawSCO(int i) {
		return variableValues[i];
	}

	public void setField(ZooFieldDef fieldDef, Object val) {
		int i = fieldDef.getFieldPos();
		switch (fieldDef.getJdoType()) {
		case ARRAY:
		case BIG_DEC:
		case BIG_INT:
		case NUMBER:
			throw new UnsupportedOperationException();
		case DATE:
			fixedValues[i] = ((Date)val).getTime();
			variableValues[i] = val;
			break;
		case PRIMITIVE:
			try {
				//this ensures the correct type
				Object x;
				switch (fieldDef.getPrimitiveType()) {
				case BOOLEAN: x = val; break;
				case BYTE: x = (Byte)val; break;
				case CHAR: x = (Character)val; break;
				case DOUBLE: x = (Double)val; break;
				case FLOAT: x = (Float)val; break;
				case INT: x = (Integer)val; break;
				case LONG: x = (Long)val; break;
				case SHORT: x = (Short)val; break;
				default: throw new IllegalStateException();
				}
				fixedValues[i] = x;
				break;
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("Value is of wrong type. Expected " +
						fieldDef.getPrimitiveType() + " but was " + val.getClass());
			}
		case REFERENCE:
			if (val != null) {
				if (val instanceof ZooPC) {
					fixedValues[i] = ((ZooPC)val).jdoZooGetOid();
				} else if (val instanceof GenericObject) {
					fixedValues[i] = ((GenericObject)val).getOid();
				} else if (val instanceof GOProxy) {
					fixedValues[i] = ((GOProxy)val).go.getOid();
				} else if (val instanceof ZooHandleImpl) {
					val = ((ZooHandleImpl)val).getGenericObject();
					fixedValues[i] = ((GenericObject)val).getOid();
				} else {
					throw DBLogger.newUser("Illegal argument type: " + val.getClass());
				}
			}
			variableValues[i] = val;
			break;
		case SCO:
			throw new UnsupportedOperationException();
		case STRING:
			fixedValues[i] = BitTools.toSortableLong((String)val);
			variableValues[i] = val;
			break;
		}
	}

	/**
	 * 
	 * Returns OIDs for references.
	 * 
	 * @param fieldDef Field definition
	 * @return The value of that field.
	 */
	public Object getField(ZooFieldDef fieldDef) {
		int i = fieldDef.getFieldPos();
		switch (fieldDef.getJdoType()) {
		case ARRAY:
		case BIG_DEC:
		case BIG_INT:
			return variableValues[i];
		case DATE:
			return new Date((Long)fixedValues[i]);
		case NUMBER:
			if (fixedValues[i] == null) {
				return null;
			}
			//return fixedValues[i];
			throw new UnsupportedOperationException(fieldDef.getTypeName());
		case PRIMITIVE: return fixedValues[i];
		case REFERENCE: return variableValues[i];
		case SCO:
		case STRING:
			return variableValues[i];
			default: throw new IllegalArgumentException();
		}
	}

	public ZooClassDef ensureLatestVersion() {
	    while (defCurrent.getNextVersion() != null) {
	        evolve();
	    }
	    return defCurrent;
	}
	
	private ZooClassDef evolve() {
		//TODO this is horrible!!!
		ArrayList<Object> fV = new ArrayList<>(Arrays.asList(fixedValues));
		ArrayList<Object> vV = new ArrayList<>(Arrays.asList(variableValues));
		
		//TODO resize only once to correct size
		for (PersistentSchemaOperation op: jdoZooGetClassDef().getNextVersion().getEvolutionOps()) {
			if (op.isAddOp()) {
				fV.add(op.getFieldId(), op.getInitialValue());
				vV.add(op.getFieldId(), null);
			} else {
				fV.remove(op.getFieldId());
				vV.remove(op.getFieldId());
			}
		}
		fixedValues = fV.toArray(fixedValues);
		variableValues = vV.toArray(variableValues);
		defCurrent = defCurrent.getNextVersion();
		//We call this just to set the context/ZooClassDef. The state can be ignored since
		//it is only a temporary object and not in the cache
		jdoZooInit(ObjectState.PERSISTENT_DIRTY, defCurrent.getProvidedContext(), 
				jdoZooGetOid());
		return defCurrent;
	}

	/**
	 * Schema evolution of in-memory objects, operation by operation.
	 * @param op Schema operation object
	 */
	public void evolve(SchemaOperation op) {
		//TODO this is horrible!!!
		ArrayList<Object> fV = new ArrayList<Object>(Arrays.asList(fixedValues));
		ArrayList<Object> vV = new ArrayList<Object>(Arrays.asList(variableValues));
		
		//TODO resize only once to correct size
		if (op instanceof SchemaOperation.SchemaFieldDefine) {
			SchemaOperation.SchemaFieldDefine op2 = (SchemaFieldDefine) op;
			int fieldId = op2.getField().getFieldPos();
			fV.add(fieldId, PersistentSchemaOperation.getDefaultValue(op2.getField()));
			vV.add(fieldId, null);
		}
		if (op instanceof SchemaOperation.SchemaFieldDelete) {
			SchemaOperation.SchemaFieldDelete op2 = (SchemaFieldDelete) op;
			int fieldId = op2.getField().getFieldPos();
			fV.remove(fieldId);
			vV.remove(fieldId);
		}
		fixedValues = fV.toArray(fixedValues);
		variableValues = vV.toArray(variableValues);
	}

	public ObjectReader toStream() {
	    System.err.println("FIXME: Size of generic object writer");
	    //TODO, this is so dirty...  If the buffer is to small, we retry with a bigger one
	    int size = 1000;
	    ByteBuffer ba = null;
	    while (ba == null) {
		    try {
		    	GenericObjectWriter gow = new GenericObjectWriter(size, 
		    			jdoZooGetClassDef().getOid(), jdoZooGetTimestamp());
				gow.newPage();
				DataSerializer ds = new DataSerializer(gow, 
				        jdoZooGetContext().getSession().internalGetCache(), 
				        jdoZooGetNode());
				ds.writeObject(this, jdoZooGetClassDef());
				ba = gow.toByteArray();
		    } catch (BufferOverflowException e) {
		    	//ignore, hehe...
			    size = size * 10;
			    System.err.println("FIXME: Resizing buffer!!!! " + size);
		    }
	    }
		ba.rewind();
		return new ObjectReader(new GenericObjectReader(ba));
	}

	public long getOid() {
		return jdoZooGetOid();
	}

    public void setOid(long oid) {
        jdoZooSetOid(oid);
    }

    public ZooClassDef getClassDefCurrent() {
        return defCurrent;
    }

    public ZooClassDef getClassDefOriginal() {
        return defOriginal;
    }

	public void setClassDefOriginal(ZooClassDef defOriginal) {
		this.defOriginal = defOriginal;
	}

	public boolean isDbCollection() {
		return isDbCollection;
	}

	/**
	 * This is used to represent DBCollection objects as GenericObjects.
	 * @param collection collection object
	 */
	public void setDbCollection(Object collection) {
		if (collection != null) {
			dbCollectionData = collection;
			isDbCollection = true;
		} else {
			dbCollectionData = null;
			isDbCollection = false;
		}
		
	}

	public Object getDbCollection() {
		return dbCollectionData;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof GenericObject)) {
			return false;
		}
		System.out.println("FIXME: compare values");
		return true;
	}
	
	public ZooHandleImpl getOrCreateHandle() {
		if (handle == null) {
			handle = new ZooHandleImpl(this, defOriginal.getVersionProxy());
		}
		return handle;
	}

	public void activateRead() {
		//TODO use ZooDB method??
		if (jdoZooIsStateHollow()) {
			GenericObject go = jdoZooGetNode().readGenericObject(
					jdoZooGetClassDef(), jdoZooGetOid());
			if (go != this) {
				throw DBLogger.newFatalInternal("Arrgh!");
			}
		}
	}

	/**
	 * This method verifies that this GO has no PCI representation or that the PCI representation
	 * is not dirty or new.
	 * Otherwise it will throw an exception in order to prevent the dirty state of the GO and the
	 * PC to result in conflicting updates in the database.
	 */
	void verifyPcNotDirty() {
		if (handle == null || handle.internalGetPCI() == null || 
				!handle.internalGetPCI().jdoZooIsDirty()) {
			ZooPC pc = jdoZooGetContext().getSession().internalGetCache().findCoByOID(
					jdoZooGetOid());
			if (pc == null || !pc.jdoZooIsDirty()) {
				return;
			}
		}
		throw DBLogger.newUser("This object has been modified via its Java class as well as via" +
				" the schema API. This is not allowed. Object ID: " +
				Util.oidToString(jdoZooGetOid()));
	}
	
	/**
	 * @return True if there is an associated PC that is deleted.
	 */
	public boolean checkPcDeleted() {
		if (handle == null || handle.internalGetPCI() == null || 
				!handle.internalGetPCI().jdoZooIsDeleted()) {
			ZooPC pc = jdoZooGetContext().getSession().internalGetCache().findCoByOID(
					jdoZooGetOid());
			if (pc == null || !pc.jdoZooIsDeleted()) {
				return false;
			}
		}
		return true;
	}

	public Object[] getRawFields() {
		return fixedValues;
	}

	public void invalidate() {
		handle.invalidate();
	}
}
