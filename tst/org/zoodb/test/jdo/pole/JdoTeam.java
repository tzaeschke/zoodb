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

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;


public class JdoTeam {
    
    public void setUp(PersistenceManager pm) {
//		for(int i = 0; i < mCars.length;i++){		
//		    JdoCar jdoCar = (JdoCar)mCars[i];
//			PersistenceManager pm = jdoCar.getPersistenceManager();
		    deleteAll(pm);
//		    pm.close();
//		}
	}


	public void deleteAll(PersistenceManager pm) {
		deleteAll(pm, ComplexHolder4.class);
		deleteAll(pm, ComplexHolder3.class);
		deleteAll(pm, ComplexHolder2.class);
		deleteAll(pm, ComplexHolder1.class);
		deleteAll(pm, ComplexHolder0.class);
		
		deleteAll(pm, InheritanceHierarchy4.class);
		deleteAll(pm, InheritanceHierarchy3.class);
		deleteAll(pm, InheritanceHierarchy2.class);
		deleteAll(pm, InheritanceHierarchy1.class);
		deleteAll(pm, InheritanceHierarchy0.class);
		
		deleteAll(pm, JdoIndexedObject.class);
		deleteAll(pm, ListHolder.class);

		// old courses
		deleteAll(pm, JB0.class);
		deleteAll(pm, JB1.class);
		deleteAll(pm, JB2.class);
		deleteAll(pm, JB3.class);
		deleteAll(pm, JB4.class);
		deleteAll(pm, JdoIndexedPilot.class);
		deleteAll(pm, JdoPilot.class);
		deleteAll(pm, JdoTree.class);
		deleteAll(pm, JdoLightObject.class);
		deleteAll(pm, JdoListHolder.class);
		deleteAll(pm, JN1.class);
	}


	private void deleteAll(PersistenceManager pm, Class<?> clazz) {
		pm.currentTransaction().begin();
		ZooClass zc = ZooJdoHelper.schema(pm).getClass(clazz);
		pm.currentTransaction().commit();
		if (zc == null) {
			return;
		}
		//checkExtentSize(pm, clazz,"");
		
		// 1. try Query.deletePersistentAll()
		//deletePersistentAll(pm,clazz);
		
		//2. try PersistenceManager.delete(Extent.iterator().next()) with batches 
		deleteAllBatched(pm, clazz);
		
		//checkExtentSize(pm, clazz,"");
	}


//	private void deletePersistentAll(PersistenceManager pm, Class<?> clazz) {
//		pm.currentTransaction().begin();
//		pm.newQuery(pm.getExtent(clazz,false)).deletePersistentAll();
//		pm.currentTransaction().commit();
//	}
//	
//	private void checkExtentSize(PersistenceManager pm, Class<?> clazz, String msg){
//		pm.currentTransaction().begin();
//		Collection<?> collection = (Collection<?>) pm.newQuery(pm.getExtent(clazz,false)).execute();
//		System.out.println(msg + " " + clazz.getSimpleName() + " size: " + collection.size());
//		pm.currentTransaction().rollback();
//	}

	private void deleteAllBatched(PersistenceManager pm, Class<?> clazz) {
	    pm.currentTransaction().begin();
	    int batchSize = 10000;
	    int commitctr = 0;
	    Extent<?> extent = pm.getExtent(clazz,false);
	    Iterator<?> it = extent.iterator();
	    while(it.hasNext()){
	    	pm.deletePersistent(it.next());
	    	if ( batchSize > 0  &&  ++commitctr >= batchSize){
	    		commitctr = 0;
	    		pm.currentTransaction().commit();
	    		pm.currentTransaction().begin();
	    	}
	    }
	    extent.closeAll();
	    pm.currentTransaction().commit();
	}

}
