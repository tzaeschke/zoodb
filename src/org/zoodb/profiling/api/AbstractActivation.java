package org.zoodb.profiling.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.analyzer.PathItem;
import org.zoodb.profiling.analyzer.ReferenceShortcutAnalyzerP;
import org.zoodb.profiling.api.impl.ProfilingManager;


public class AbstractActivation {
	
	/**
	 * The field in the parent-class which triggered the activation
	 */
	private String parentFieldName;

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
	 * Associated class
	 */
	private transient Class<?> clazz;

	
	public String getParentFieldName() {
		return parentFieldName;
	}
	
	public void setParentFieldName(String parentFieldName) {
		this.parentFieldName = parentFieldName;
	}
	
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
		return parent != null ? parent.getClazz() : null;
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
	
	public Collection<AbstractActivation> getChildren() {
		return children;
	}


	public void startEvaluation(ReferenceShortcutAnalyzerP rsa, AbstractActivation rootActivation) {
		if (getChildrenCount() == 1) { 
			char fas = evaluateFieldAccess();
			
			if (fas != 0) {
				//this is a save intermediate node, continue on the child
				List<PathItem> pItems = new LinkedList<PathItem>();
				
				PathItem pi = new PathItem();
				pi.setC(clazz);
				pi.setFieldName(parentFieldName);
				pi.setTraversalCount(1);
				
				pItems.add(pi);
				
				
				children.iterator().next().doEvaluation(rsa, rootActivation.getClazz(),pItems);
			}
		} else {
			//path ends here
		}	
	}
	
	public void doEvaluation(ReferenceShortcutAnalyzerP rsa, Class<?> start, List<PathItem> pItems) {
		if (getChildrenCount() == 1) {
			char fas = evaluateFieldAccess();
			
			if (fas != 0) {
				//add itself to list and continue with child
				PathItem pi = new PathItem();
				pi.setC(clazz);
				pi.setFieldName(parentFieldName);
				pi.setTraversalCount(1);
				
				pItems.add(pi);
				
				children.iterator().next().doEvaluation(rsa, start,pItems);
			}
		} else {
			//this is an end node, put candidate
			rsa.putCandidate(start,this.getClazz(),pItems,trx);
		}
	}
	
	/**
	 * Evaluates the field access for this activation
	 * Returns one of {0,1,2} which encodes the following semantics:
	 * 
	 * 	0: this activation has 0 or more than 1 access (path will end here)
	 *  1: this activation has 1 read access 
	 *  2: this activation has 1 write access
	 * 
	 * @return
	 */
	private char evaluateFieldAccess() {
		Collection<IFieldAccess> fas = ProfilingManager.getInstance().getFieldManager().get(this.getOid(), this.getTrx());
		
		if (fas.size() == 1) {
			if (fas.iterator().next().isWrite()) {
				return 2;
			} else {
				return 1;
			}
		} else {
			return 0;
		}
	}
	
	public boolean hasWriteAccess() {
		Collection<IFieldAccess> fas = ProfilingManager.getInstance().getFieldManager().get(this.getOid(), this.getTrx());
		
		if (fas.size() > 0) {
			for (IFieldAccess fa : fas) {
				if (fa.isWrite()) return true;
			}
			return false;
		} else {
			return false;
		}

	} 

}