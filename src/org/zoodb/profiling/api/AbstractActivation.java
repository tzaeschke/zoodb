package org.zoodb.profiling.api;


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
	

}