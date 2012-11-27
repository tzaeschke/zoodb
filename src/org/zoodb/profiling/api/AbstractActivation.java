package org.zoodb.profiling.api;

import java.util.Collection;
import java.util.LinkedList;


public class AbstractActivation {
	
	/**
	 * OID of this activated object 
	 */
	private long oid;
	
	/**
	 * Size in bytes of this activation 
	 */
	private long bytes;
	
	/**
	 * Trx in which this activation took place 
	 */
	private String trx;
	
	/**
	 * Parent activation, necessary for chaining and path-analysis 
	 */
	private AbstractActivation parent;
	
	/**
	 * Every item in 'children' has (oid,trx) as its predecessor
	 */
	private Collection<AbstractActivation> children;
	
	/**
	 * Associatec class
	 */
	private transient Class<?> clazz;


	private transient Class<?> parentClass;
	
	private transient long parentOid;
	
	
	
	
	public long getOid() {
		return oid;
	}


	public void setOid(long oid) {
		this.oid = oid;
	}


	public long getBytes() {
		return bytes;
	}


	public void setBytes(long bytes) {
		this.bytes = bytes;
	}


	public String getTrx() {
		return trx;
	}


	public void setTrx(String trx) {
		this.trx = trx;
	}


	public AbstractActivation getParent() {
		return parent;
	}


	public void setParent(AbstractActivation parent) {
		this.parent = parent;
	}
	
	
	
	public Class<?> getClazz() {
		return clazz;
	}


	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	

	public Class<?> getParentClass() {
		return parentClass;
	}


	public void setParentClass(Class<?> parentClass) {
		this.parentClass = parentClass;
	}


	public long getParentOid() {
		return parentOid;
	}


	public void setParentOid(long parentOid) {
		this.parentOid = parentOid;
	}


	public void addChildren(AbstractActivation a) {
		if (children == null) {
			children = new LinkedList<AbstractActivation>();
		}
		children.add(a);
	}
	

}