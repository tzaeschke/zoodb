package org.zoodb.jdo.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DBVector<E> extends PersistenceCapableImpl implements List<E> {
	private transient Vector<E> _v;

	public DBVector() {
		_v = new Vector<E>();
	}
	
	public DBVector(int size) {
		_v = new Vector<E>(size);
	}
	
	@Override
	public boolean add(E e) {
		return _v.add(e);
	}

	@Override
	public void add(int index, E element) {
		_v.add(index, element);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return _v.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		return _v.addAll(index, c);
	}

	@Override
	public void clear() {
		_v.clear();
	}

	@Override
	public boolean contains(Object o) {
		return _v.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return _v.containsAll(c);
	}

	@Override
	public E get(int index) {
		return _v.get(index);
	}

	@Override
	public int indexOf(Object o) {
		return _v.indexOf(o);
	}

	@Override
	public boolean isEmpty() {
		return _v.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return _v.iterator();
	}

	@Override
	public int lastIndexOf(Object o) {
		return _v.lastIndexOf(o);
	}

	@Override
	public ListIterator<E> listIterator() {
		return _v.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		return _v.listIterator(index);
	}

	@Override
	public boolean remove(Object o) {
		return _v.remove(o);
	}

	@Override
	public E remove(int index) {
		return _v.remove(index);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return _v.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return _v.retainAll(c);
	}

	@Override
	public E set(int index, E element) {
		return _v.set(index, element);
	}

	@Override
	public int size() {
		return _v.size();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return _v.subList(fromIndex, toIndex);
	}

	@Override
	public Object[] toArray() {
		return _v.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return _v.toArray(a);
	}

	public void setBatchSize(int i) {
		System.out.println("STUB: DBVector.setBatchSize()");
	}

	public void resize(int size) {
		System.out.println("STUB: DBVector.resize()");
	}
}
