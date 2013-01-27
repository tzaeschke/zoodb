package org.zoodb.profiling.api;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.zoodb.profiling.analyzer.ReferenceShortcutAnalyzerP;
import org.zoodb.profiling.api.impl.ProfilingManager;


public class AbstractActivation {
	
	/**
	 * The field in the parent-class which triggered the activation
	 */
	private String parentFieldName;

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
	 * Associated class
	 */
	private transient Class<?> clazz;


	private transient Class<?> parentClass;
	
	private transient long parentOid;
	
	/**
	 * The pageId this object was located upon activation 
	 */
	private int pageId;
	
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
	
	public Collection<AbstractActivation> getChildren() {
		return children;
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
		if (getChildrenCount() == 1) { 
			char fas = evaluateFieldAccess();
			
			if (fas != 0) {
				//this is a save intermediate node, continue on the child
				List<Class<?>> intermediates = new LinkedList<Class<?>>();
				List<Long> intermediateSize = new LinkedList<Long>();
				Set<Integer> intermediateWritePages = null;
				intermediates.add(clazz);
				intermediateSize.add(bytes);
				
				if (rootActivation.hasWriteAccess()) {
					intermediateWritePages = new HashSet<Integer>();
					intermediateWritePages.add(rootActivation.getPageId());
				}
				
				// remember the pageId for write
				if (fas == 2) {
					if (intermediateWritePages == null) {
						intermediateWritePages = new HashSet<Integer>();
					}
					intermediateWritePages.add(pageId);
				} 
				
				children.iterator().next().doEvaluation(rsa, intermediates,intermediateSize,intermediateWritePages,rootActivation.getClazz());
			}
		} else {
			//path ends here
		}	
	}
	
	public void doEvaluation(ReferenceShortcutAnalyzerP rsa, List<Class<?>> intermediates, List<Long> intermediateSize,Set<Integer> intermediateWritePages,Class<?> start) {
		if (getChildrenCount() == 1) {
			char fas = evaluateFieldAccess();
			
			if (fas != 0) {
				//add itself to list and continue with child
				intermediates.add(clazz);
				intermediateSize.add(bytes);
				
				// remember the pageId for write
				if (fas == 2) {
					if (intermediateWritePages == null) {
						intermediateWritePages = new HashSet<Integer>();
					}
					intermediateWritePages.add(pageId);
				}
				
				children.iterator().next().doEvaluation(rsa, intermediates,intermediateSize,intermediateWritePages,start);
			}
		} else {
			//this is an end node, put candidate
			rsa.putCandidate(start,this.getClazz(),intermediates,intermediateSize,intermediateWritePages,trx);
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

	public int getPageId() {
		return pageId;
	}

	public void setPageId(int pageId) {
		this.pageId = pageId;
	}
	
	/**
	 * Returns the field in the parent-class which was traversed before this activation was triggered. 
	 * @return
	 */
	public Field getParentField() {
		if (parent != null) {
			//the field is the one which was _last_ accessed right before this activation occured
			//timestamp comparison
			Collection<IFieldAccess> parentFA = ProfilingManager.getInstance().getFieldManager().get(parent.getOid(), parent.getTrx());
			IFieldAccess match = null;
			long distance = Long.MAX_VALUE;
			for (IFieldAccess fa : parentFA) {
				if (this.timestamp - fa.getTimestamp() < distance) {
					distance = this.timestamp - fa.getTimestamp();
					match = fa;
				}
			}
			if (match != null) {
				try {
					Field f = parent.getClazz().getDeclaredField(match.getFieldName());
					f.setAccessible(true);
					return f;
				} catch (NoSuchFieldException e) {
					e.printStackTrace();
				} catch (SecurityException e) {
					e.printStackTrace();
				}
			} 
		} 
		return null;
	}

}