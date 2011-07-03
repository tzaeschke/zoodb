package org.zoodb.jdo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.PersistenceManager;

/**
 * This class implements JDO behavior for the class Extend.
 * @param <T>
 * 
 * @author Tilmann Zäschke
 */
public class ExtentImpl<T> implements Extent<T> {
    
    private final Class<T> _class;
    private final boolean _subclasses;
    private final List<ExtentIterator<T>> _allIterators = new LinkedList<ExtentIterator<T>>();
    private final PersistenceManagerImpl _pManager;
    private final Class<?> _minClass;
    
    /**
     * @param persistenceCapableClass
     * @param subclasses
     * @param pm
     */
    public ExtentImpl(Class<T> persistenceCapableClass, 
            boolean subclasses, PersistenceManagerImpl pm, Class<?> minClass) {
        _class = persistenceCapableClass;
        _subclasses = subclasses;
        _pManager = pm;
        _minClass = minClass;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#iterator()
     */
    public Iterator<T> iterator() {
    	@SuppressWarnings("unchecked")
		Iterator<T> it = 
    		(Iterator<T>) _pManager.getSession().loadAllInstances(_class, _subclasses, _minClass);
    	ExtentIterator<T> eIt = new ExtentIterator<T>(it); 
    	_allIterators.add(eIt);
    	return eIt;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#close(java.util.Iterator)
     */
    public void close(Iterator<T> i) {
        ExtentIterator.class.cast(i).close();
        _allIterators.remove(i);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#closeAll()
     */
    public void closeAll() {
        for (ExtentIterator<T> i: _allIterators) {
            i.close();
        }
        _allIterators.clear();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#hasSubclasses()
     */
    public boolean hasSubclasses() {
        return _subclasses;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#getPersistenceManager()
     */
    public PersistenceManager getPersistenceManager() {
        return _pManager;
    }
    
	@Override
	public Class<T> getCandidateClass() {
		return _class;
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
