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


package org.zoodb.test.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

import javax.jdo.JDOHelper;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ComplexHolder0 extends PersistenceCapableImpl implements CheckSummable {
	
	private long id;
	
	private String name;
	
	private List<ComplexHolder0> children = new ArrayList<ComplexHolder0>();
	
	private ComplexHolder0[] array;
	
	public ComplexHolder0() {
		
	}
	
	public static ComplexHolder0 generate(int depth, int leafs, boolean disjunctSpecial){
		ComplexHolder0 complexHolder = new ComplexHolder0();
		
		complexHolder.name = "root";
		int specialValue = disjunctSpecial ? (int) Math.pow(leafs, depth) : 0;
		createChildren(complexHolder, depth -1, leafs, specialValue);
		return complexHolder;
	}
	
	private static void createChildren(ComplexHolder0 root, int depth, int numChildren, int specialValue) {
		if(depth < 1){
			return;
		}
		int factoryIdx = 0;
		int holderIdx = 0;
		List<ComplexHolder0> parentLevel = Arrays.asList(root);
		for (int i = 0; i < depth; i++) {
			Closure4<ComplexHolder0> curFactory = FACTORIES[factoryIdx];
			List<ComplexHolder0> childLevel = new ArrayList<ComplexHolder0>();

			for (ComplexHolder0 curParent : parentLevel) {
				for (int childIdx = 0; childIdx < numChildren; childIdx++) {
					ComplexHolder0 curChild = curFactory.run();
					curChild.name = String.valueOf(holderIdx);
					curChild.array = createArray(holderIdx);
					curChild.setSpecial(holderIdx + specialValue);
					curParent.addChild(curChild);
					childLevel.add(curChild);
					holderIdx++;
				}
			}

			parentLevel = childLevel;
			
			factoryIdx++;
			if(factoryIdx == FACTORIES.length) {
				factoryIdx = 0;
			}
		}
		
	}

	private static ComplexHolder0[] createArray(int holderIdx) {
		ComplexHolder0[] holders = new ComplexHolder0[] {
			new ComplexHolder0(),
			new ComplexHolder1(),
			new ComplexHolder2(),
			new ComplexHolder3(),
			new ComplexHolder4(),
		};
		for (int i = 0; i < holders.length; i++) {
			holders[i].name = "a" + holderIdx + "_" + i;
		}
		return holders;
	}

	public void addChild(ComplexHolder0 child) {
        zooActivate(this);
		children.add(child);
	}


	public static final Closure4[] FACTORIES = {
		new Closure4<ComplexHolder0>(){
			@Override
			public ComplexHolder0 run() {
				return new ComplexHolder0();
			}
		},
		new Closure4<ComplexHolder0>(){
			@Override
			public ComplexHolder0 run() {
				return new ComplexHolder1();
			}
		},
		new Closure4<ComplexHolder0>(){
			@Override
			public ComplexHolder0 run() {
				return new ComplexHolder2();
			}
		},
		new Closure4<ComplexHolder0>(){
			@Override
			public ComplexHolder0 run() {
				return new ComplexHolder3();
			}
		},
		new Closure4<ComplexHolder0>(){
			@Override
			public ComplexHolder0 run() {
				return new ComplexHolder4();
			}
		}
	};

	@Override
	public long checkSum() {
        zooActivate(this);
		
		class CheckSumVisitor implements Visitor<ComplexHolder0> {
			
			long checkSum;
			
			@Override
			public void visit(ComplexHolder0 holder) {
				checkSum += Math.abs(holder.ownCheckSum());
			}
		}
		CheckSumVisitor visitor = new CheckSumVisitor();
		traverse(visitor, new NullVisitor<ComplexHolder0>());
		return visitor.checkSum;
	}

	public void traverse(Visitor<ComplexHolder0> preVisitor, Visitor<ComplexHolder0> postVisitor) {
		internalTraverse(new IdentityHashMap<ComplexHolder0, ComplexHolder0>(), preVisitor, postVisitor);
	}

	private void internalTraverse(IdentityHashMap<ComplexHolder0, ComplexHolder0> visited, Visitor<ComplexHolder0> preVisitor, Visitor<ComplexHolder0> postVisitor) {
		if(visited.containsKey(this)) {
			return;
		}
		visited.put(this, this);
		preVisitor.visit(this);
		for (ComplexHolder0 child : getChildren()) {
			child.internalTraverse(visited, preVisitor, postVisitor);
		}
		if(getArray() != null) {
			for (ComplexHolder0 child : getArray()) {
				child.internalTraverse(visited, preVisitor, postVisitor);
			}
		}
		postVisitor.visit(this);
	}
	

	public long ownCheckSum() {
		return getName().hashCode();
	}

	protected void setSpecial(int value) {
	}
	
	public String getName() {
	    zooActivate(this);
		return name;
	}

	public void setName(String name) {
        zooActivate(this);
		this.name = name;
		JDOHelper.makeDirty(this, "");
	}

	public List<ComplexHolder0> getChildren() {
        zooActivate(this);
        if (children != null) {
	        for (ComplexHolder0 c: children) {
	        	zooActivate(c);
	        }
        }
		return children;
	}
	
	public void setChildren(List<ComplexHolder0> children) {
        zooActivate(this);
		this.children = children;
		JDOHelper.makeDirty(this, "");
	}
	
	public ComplexHolder0[] getArray() {
        zooActivate(this);
        if (array != null) {
	        for (ComplexHolder0 c: array) {
	        	zooActivate(c);
	        }
        }
		return array;
	}

	public void setArray(ComplexHolder0[] array) {
        zooActivate(this);
		this.array = array;
		JDOHelper.makeDirty(this, "");
	}


}
