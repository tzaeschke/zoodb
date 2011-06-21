package org.zoodb.jdo.internal.server;

import java.lang.reflect.Field;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;
import org.zoodb.jdo.internal.server.index.CloseableIterator;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;

/**
 * TODO
 * This class can be improved in various ways:
 * a) Implement batch loading
 * b) Start a second thread that loads the next object after the previous one has been 
 *    delivered. 
 * c) Implement this iterator also in other reader classes.
 * 
 * @author Tilmann Zäschke
 */
public class ObjectIterator implements CloseableIterator<PersistenceCapableImpl> {

	private final CloseableIterator<LLEntry> iter;  
	private final DiskAccessOneFile file;
	private final ZooClassDef clsDef;
	private final ZooFieldDef field;
	private final LongLongIndex index;
	private final DataDeSerializer deSer;
	
	/**
	 * Object iterator.
	 * 
	 * The last three fields can be null. If they are, the objects are simply returned and no checks
	 * are performed.
	 * 
	 * @param iter
	 * @param cache
	 * @param file
	 * @param clsDef Can be null.
	 * @param field Can be null.
	 * @param fieldInd Can be null.
	 */
	public ObjectIterator(CloseableIterator<LLEntry> iter, AbstractCache cache, 
			DiskAccessOneFile file, ZooClassDef clsDef, ZooFieldDef field, LongLongIndex fieldInd, 
			SerialInput in, Node node) {
		this.iter = iter;
		this.file = file;
		this.clsDef = clsDef;
		this.field = field;
		this.index = fieldInd;
		this.deSer = new DataDeSerializer(in, cache, node);
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public PersistenceCapableImpl next() {
		LLEntry e = iter.next();
		PersistenceCapableImpl pc = file.readObject(deSer, e.getValue());
		if (index == null) {
			return pc;
		}
		while (!checkObject(e, pc)) {
			//TODO this is gonna fail if the last element if outdated!!! 
			// It can be outdated in normal indices because we do not directly remove entries
			// when they change, we remove them only when they are loaded and do not match anymore.
			// -> This is a problem when we rely on the index to get a count of matching objects.
			DatabaseLogger.debugPrintln(1, "Found outdated index entry for " + 
					Util.oidToString(e.getValue()));
			index.removeLong(e.getKey(), e.getValue());
			e = iter.next();
			pc = file.readObject(deSer, e.getValue());
		}
		return pc;
	}

	@Override
	public void remove() {
		// do we need this? Should we allow it? I guess it fails anyway in the LLE-iterator.
		iter.remove();
	}
	
	private boolean checkObject(LLEntry entry, PersistenceCapableImpl pc) {
		Class<?> jCls = null;
		Field jField = null;
		try {
			jCls = Class.forName(clsDef.getClassName());
			jField = jCls.getDeclaredField(field.getName());
			switch (field.getPrimitiveType()) {
			case BOOLEAN:
				return entry.getValue() == (jField.getBoolean(pc) ? 1 : 0);
			case BYTE: 
				return entry.getValue() == jField.getByte(pc);
			case DOUBLE: 
	    		System.out.println("STUB DiskAccessOneFile.writeObjects(DOUBLE)");
	    		//TODO
//				return entry.getValue() == jField.getDouble(pc);
			case FLOAT:
				//TODO
	    		System.out.println("STUB DiskAccessOneFile.writeObjects(FLOAT)");
//				return entry.getValue() == jField.getFloat(pc);
			case INT: 
				return entry.getValue() == jField.getInt(pc);
			case LONG: 
				return entry.getValue() == jField.getLong(pc);
			case SHORT: 
				return entry.getValue() == jField.getShort(pc);
			default:
				throw new IllegalArgumentException("type = " + field.getPrimitiveType());
			}
		} catch (ClassNotFoundException e) {
			throw new JDOFatalDataStoreException(
					"Class not found: " + clsDef.getClassName(), e);
		} catch (SecurityException e) {
			throw new JDOFatalDataStoreException(
					"Error accessing field: " + field.getName(), e);
		} catch (NoSuchFieldException e) {
			throw new JDOFatalDataStoreException("Field not found: " + field.getName(), e);
		} catch (IllegalArgumentException e) {
			throw new JDOFatalDataStoreException(
					"Error accessing field: " + field.getName(), e);
		} catch (IllegalAccessException e) {
			throw new JDOFatalDataStoreException(
					"Error accessing field: " + field.getName(), e);
		}
	}
	
	@Override
	public void close() {
		iter.close();
	}
	
	@Override
	protected void finalize() throws Throwable {
		iter.close();
		super.finalize();
	}
}
