/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */


package org.zoodb.test.jdo.pole;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zoodb.api.impl.ZooPCImpl;

public class ListHolder extends ZooPCImpl implements CheckSummable {
	
	interface Procedure<T> {
		
		void apply(T obj);

	}

	public static final String ROOT_NAME = "root";
	
	private static IdGenerator _idGenerator = new IdGenerator();
	
	private long _id;

	private String _name;
	
	private List<ListHolder> _list;
	
	private static int n = 0;

	private ListHolder() {
		n++;
	}
	
	public static ListHolder generate(int depth, int leafs, int reuse){
		ListHolder root = generate(new ArrayList<ListHolder>(), depth, leafs, reuse);
		root._name = ROOT_NAME;
		System.out.println("nC=" + n);
		return root;
	}
	
	private static ListHolder generate(List<ListHolder> flatList, int depth, int leafs, int reuse){
		if(depth == 0){
			return null;
		}
		ListHolder listHolder = new ListHolder();
		listHolder.setId(_idGenerator.nextId());
		
		flatList.add(listHolder);
		if(depth == 1){
			return listHolder;
		}
		listHolder.setList(new ArrayList<ListHolder>());
		int childDepth = depth -1;
		for (int i = leafs -1; i >= 0; i--) {
			if(i < reuse){
				int indexInList = (flatList.size() - i) / 2;
				if(indexInList < 0){
					indexInList = 0;
				}
				listHolder.getList().add(flatList.get(indexInList) );
			} else {
				ListHolder child = generate(flatList, childDepth, leafs, reuse);
				child._name = "child:" + depth + ":" + i;
				listHolder.getList().add(child);
			}
		}
		return listHolder;
	}

	@Override
	public long checkSum() {
        zooActivateRead();
		return _name.hashCode();
	}

	public void accept(Visitor<ListHolder> visitor) {
        zooActivateRead();
		Set<ListHolder> visited = new HashSet<ListHolder>();
		acceptInternal(visited, visitor);
	}
	
	private void acceptInternal(Set<ListHolder> visited, Visitor<ListHolder> visitor){
		zooActivateRead();
		if(visited.contains(this)){
			return;
		}
		visitor.visit(this);
		visited.add(this);
		if(getList() == null){
			return;
		}
		Iterator<ListHolder> i = getList().iterator();
		while(i.hasNext()){
			ListHolder child = i.next();
			child.acceptInternal(visited, visitor);
		}
	}
	
	public int update(int maxDepth, Procedure<ListHolder> storeProcedure) {
    	zooActivateWrite();
		Set<ListHolder> visited = new HashSet<ListHolder>();
		return updateInternal(visited, maxDepth, 0, storeProcedure);
	}


	private int updateInternal(Set<ListHolder> visited, int maxDepth, int depth, Procedure<ListHolder> storeProcedure) {
    	zooActivateWrite();
		if(visited.contains(this)){
			return 0;
		}
		visited.add(this);
		int updatedCount = 1;
		if(depth > 0){
			_name = "updated " + _name;
		}
		
		if(_list != null){
			for (int i = 0; i < _list.size(); i++) {
				ListHolder child = _list.get(i);
				updatedCount += child.updateInternal(visited, maxDepth, depth +  1, storeProcedure);
			}
		}
		storeProcedure.apply(this);
		return updatedCount;
	}

	public int delete(int maxDepth, Procedure<ListHolder> deleteProcedure) {
        zooActivateRead();
		// We use an IdentityHashMap here so hashCode is not called on deleted items.
		Map<ListHolder, ListHolder> visited = new IdentityHashMap<ListHolder, ListHolder>();
		return deleteInternal(visited, maxDepth, 0, deleteProcedure);
	}

	private int deleteInternal(Map<ListHolder, ListHolder> visited, int maxDepth, int depth, Procedure<ListHolder> deleteProcedure) {
		if(visited.containsKey(this)){
			return 0;
		}
		zooActivateRead();
		visited.put(this, this);
		int deletedCount = 1;
		if(_list != null){
			for (int i = 0; i < _list.size(); i++) {
				ListHolder child = getList().get(i);
				deletedCount += child.deleteInternal(visited, maxDepth, depth +  1, deleteProcedure);
			}
		}
		deleteProcedure.apply(this);
		return deletedCount;
	}

	private void setId(long id) {
    	zooActivateWrite();
		_id = id;
	}


	public long getId() {
        zooActivateRead();
		return _id;
	}


	private void setList(List<ListHolder> list) {
    	zooActivateWrite();
		_list = list;
	}


	private List<ListHolder> getList() {
        zooActivateRead();
		return _list;
	}
	
	@Override
	public boolean equals(Object obj) {
        zooActivateRead();
		if(this == obj){
			return true;
		}
		if(obj == null){
			return false;
		}
		if(obj.getClass() != this.getClass()){
			return false;
		}
		ListHolder other = (ListHolder) obj;
		return _id == other._id;
	}
	
	@Override
	public int hashCode() {
        zooActivateRead();
		return (int)_id;
	}

	@Override
	public String toString() {
        zooActivateRead();
		return "ListHolder [_id=" + _id + "]";
	}

}
