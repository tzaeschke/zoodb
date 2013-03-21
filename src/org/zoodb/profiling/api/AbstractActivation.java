package org.zoodb.profiling.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zoodb.profiling.analyzer.PathItem;
import org.zoodb.profiling.analyzer.ReferenceShortcutAnalyzerP;
import org.zoodb.profiling.api.impl.SimpleFieldAccess;
import org.zoodb.profiling.api.impl.Trx;


public class AbstractActivation {
	
	/**
	 * The field in the parent-class which triggered the activation
	 */
	private String parentFieldName;

	/**
	 * OID of this activated object 
	 */
	//private long oid;
	
	/**
	 * Trx in which this activation took place 
	 */
	//private String trx;
	private Trx trx;
	
	
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

	//private Map<Integer,SimpleFieldAccess> fas;
	
	private Collection<SimpleFieldAccess> fas2;
	
	public String getParentFieldName() {
		return parentFieldName;
	}
	
	public void setParentFieldName(String parentFieldName) {
		this.parentFieldName = parentFieldName;
	}
	
//	public long getOid() {
//		return oid;
//	}


//	public void setOid(long oid) {
//		this.oid = oid;
//	}


	public AbstractActivation getParent() {
		return parent;
	}


	public Trx getTrx() {
		return trx;
	}

	public void setTrx(Trx trx) {
		this.trx = trx;
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
			children = new ArrayList<AbstractActivation>();
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
			rsa.putCandidate(start,this.getClazz(),pItems,trx.getId());
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
		if (fas2 != null) {
			if (fas2.size() != 1) {
				return 0;
			} else {
				for (SimpleFieldAccess sfa : fas2) {
					if (sfa.getrCount() > 0) {
						return 1;
					} else {
						return 2;
					}
				}
			}
		}
		return 0;
	}
	
	public boolean hasWriteAccess() {
		if (fas2 != null)
		for (SimpleFieldAccess sfa : fas2) {
			if (sfa.getwCount() > 0) {
				return true;
			}
		}
		return false;
	}

	public Map<Integer,SimpleFieldAccess> getFas() {
		//return fas;
		return null;
	}
	
	public Set<Integer> getAccessedFieldIndices() {
		Set<Integer> result = new HashSet<Integer>();
		for (SimpleFieldAccess i : fas2) {
			result.add(i.getIdx());
		}
		return result;		
	}
	
	public SimpleFieldAccess getByFieldIndex(int index) {
		for (SimpleFieldAccess i : fas2) {
			if (i.getIdx() == index) {
				return i;
			}
		}
		return null;
	}
	
	public Collection<SimpleFieldAccess> getFas2() {
		return fas2;
	}
	
	
//	public void setFas(Map<Integer,SimpleFieldAccess> fas) {
//		this.fas = fas;
//	}
	public void addFieldAccess(int index, boolean read) {
		if (fas2 == null) {
			fas2 = new ArrayList<SimpleFieldAccess>();
			SimpleFieldAccess sfa = new SimpleFieldAccess();
			sfa.setIdx(index);
			if (read) {
				sfa.setrCount(1);
			} else {
				sfa.setwCount(1);
			}
			fas2.add(sfa);
		} else {
			//get the corresponding field access and update its read/write counter
			SimpleFieldAccess sfa = null;
			for (SimpleFieldAccess f : fas2) {
				if (f.getIdx() == index) {
					sfa = f;
					break;
				}
			}
			
			if (sfa == null) {
				sfa = new SimpleFieldAccess();
				sfa.setIdx(index);
				fas2.add(sfa);
			}
			if (read) {
				sfa.incR();
			} else {
				sfa.incW();
			}
		}
//		if (fas == null) {
//			fas = new HashMap<Integer,SimpleFieldAccess>();
//			SimpleFieldAccess sfa = new SimpleFieldAccess();
//			sfa.setIdx(index);
//			if (read) {
//				sfa.setrCount(1);
//			} else {
//				sfa.setwCount(1);
//			}
//			fas.put(index, sfa);
//		} else {
//			//get the corresponding field access and update its read/write counter
//			SimpleFieldAccess sfa = fas.get(index);
//			
//			if (sfa == null) {
//				sfa = new SimpleFieldAccess();
//				sfa.setIdx(index);
//				fas.put(index, sfa);
//			}
//			if (read) {
//				sfa.incR();
//			} else {
//				sfa.incW();
//			}
//		}
	}
	
	

}
