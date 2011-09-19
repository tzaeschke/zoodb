package org.zoodb.jdo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.stuff.CloseableIterator;

/**
 * This class implements JDO behavior for the class Extend.
 * @param <T>
 * 
 * @author Tilmann Zäschke
 */
public class ExtentImpl<T> implements Extent<T> {
    
    private final Class<T> extClass;
    private final boolean subclasses;
    private final List<CloseableIterator<T>> _allIterators = new LinkedList<CloseableIterator<T>>();
    private final PersistenceManagerImpl pm;
    private final boolean ignoreCache;
    
    /**
     * @param persistenceCapableClass
     * @param subclasses
     * @param pm
     */
    public ExtentImpl(Class<T> persistenceCapableClass, 
            boolean subclasses, PersistenceManagerImpl pm, boolean ignoreCache) {
    	if (!PersistenceCapable.class.isAssignableFrom(persistenceCapableClass)) {
    		throw new JDOUserException("Class is not persistence capabale: " + 
    				persistenceCapableClass.getName());
    	}
        this.extClass = persistenceCapableClass;
        this.subclasses = subclasses;
        this.pm = pm;
        this.ignoreCache = ignoreCache;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#iterator()
     */
    public Iterator<T> iterator() {
    	@SuppressWarnings("unchecked")
		CloseableIterator<T> it = (CloseableIterator<T>) pm.getSession().loadAllInstances(
    		        extClass, subclasses, !ignoreCache);
    	_allIterators.add(it);
    	return it;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#close(java.util.Iterator)
     */
    public void close(Iterator<T> i) {
        CloseableIterator.class.cast(i).close();
        _allIterators.remove(i);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#closeAll()
     */
    public void closeAll() {
        for (CloseableIterator<T> i: _allIterators) {
            i.close();
        }
        _allIterators.clear();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#hasSubclasses()
     */
    public boolean hasSubclasses() {
        return subclasses;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#getPersistenceManager()
     */
    public PersistenceManager getPersistenceManager() {
        return pm;
    }
    
	@Override
	public Class<T> getCandidateClass() {
		return extClass;
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
