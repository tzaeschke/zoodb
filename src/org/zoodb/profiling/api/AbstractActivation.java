package org.zoodb.profiling.api;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.analyzer.ReferenceShortcutAnalyzerP;
import org.zoodb.profiling.api.impl.ProfilingManager;


public class AbstractActivation {
	
	
	/**
	 * Time of activation (use for comparison with field acess of parent) 
	 */
	private long timestamp;
	
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
	
	public int getChildrenCount() {
		return children == null ? 0 : children.size();
	}
	
	public Iterator<AbstractActivation> getChildrenIterator() {
		return children != null ? children.iterator() : null;
	}


	public long getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String parentFieldName() {
		if (parent != null) {
			List<IFieldAccess> fas = (List<IFieldAccess>) ProfilingManager.getInstance().getFieldManager().get(parentOid, trx);
			
			//get the latest fieldAccess 
			Collections.sort(fas, new FieldAccessComparator());
			
			return fas.get(0).getFieldName();			
		} else {
			return null;
		}
	}
	
	public void startEvaluation(ReferenceShortcutAnalyzerP rsa, AbstractActivation rootActivation) {
		if (getChildrenCount() == 1 && evaluateFieldAccess()) {
			//this is a save intermediate node, continue on the child
			List<Class<?>> intermediates = new LinkedList<Class<?>>();
			List<Long> intermediateSize = new LinkedList<Long>();
			intermediates.add(clazz);
			intermediateSize.add(bytes);
			children.iterator().next().doEvaluation(rsa, intermediates,intermediateSize,rootActivation.getClazz());
		} else {
			//path ends here
		}	
	}
	
	public void doEvaluation(ReferenceShortcutAnalyzerP rsa, List<Class<?>> intermediates, List<Long> intermediateSize,Class<?> start) {
		if (getChildrenCount() == 1 && evaluateFieldAccess()) {
			//add itself to list and continue with child
			intermediates.add(clazz);
			intermediateSize.add(bytes);
			children.iterator().next().doEvaluation(rsa, intermediates,intermediateSize,start);
		} else {
			//this is an end node, put candidate
			rsa.putCandidate(start,this.getClazz(),intermediates,intermediateSize);
		}
	}
	
	private boolean evaluateFieldAccess() {
		Collection<IFieldAccess> fas = ProfilingManager.getInstance().getFieldManager().get(this.getOid(), this.getTrx());
		return fas.size() == 1;
	}

}