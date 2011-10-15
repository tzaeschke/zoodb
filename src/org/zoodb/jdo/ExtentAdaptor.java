package org.zoodb.jdo;

import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;

public class ExtentAdaptor<E> implements Collection<E> {

	private final Extent<E> _ext;
	
	ExtentAdaptor(Extent<E> extent) {
		_ext = extent;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public Iterator<E> iterator() {
		return _ext.iterator();
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object[] toArray(Object[] a) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public boolean add(Object e) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean containsAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean addAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean removeAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public boolean retainAll(Collection c) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}
}
