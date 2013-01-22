package org.zoodb.profiling.api;

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.jdo.api.DBCollection;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;

public class ActivationFactory {
	
	
	/**
	 * @param o
	 * @return Returns the activation object corresponding to thi
	 */
	public static AbstractActivation get(ZooPCImpl o) {
		AbstractActivation a = null;
		
		if (o instanceof DBCollection) {
			a = new CollectionActivation();
			
			if (o instanceof DBArrayList) {
				
				// this call is safe (because o is already activated at this point
				//int size = ((DBArrayList) o).size();
				int size = getSize((DBArrayList) o); 
				((CollectionActivation) a).setSize(size);
				
			}

		} else {
			// which type of activation
			a = new PCActivation();
		}
		
		a.setClazz(o.getClass());
		a.setBytes(o.getTotalReadEffort());
		a.setOid(o.jdoZooGetOid());
		a.setTrx(ProfilingManager.getInstance().getCurrentTrxId());
		a.setTimestamp(System.currentTimeMillis());
		a.setPageId(o.getPageId());
		
		/*
		 * If predecessor is not null
		 * there is already an activation in the activation archive of the predecessor
		 * 
		 * It is therefore save to find the activationArchive of predecessorsClass
		 * and insert its child
		 * 
		 */
		if (o.getActivationPathPredecessor() != null) {
			ZooPCImpl parent = o.getActivationPathPredecessor();
			
			a.setParentOid(parent.jdoZooGetOid());
			a.setParentClass(parent.getClass());
			
			ActivationArchive archive = ProfilingManager.getInstance().getPathManager().getArchive(parent.getClass());
			AbstractActivation parentActivation = archive.get(parent.jdoZooGetOid(), a.getTrx());
			
			parentActivation.addChildren(a);
			a.setParent(parentActivation);
		}
		
		
		return a;
	}
	
	private static Integer getSize(DBArrayList o) {
		Integer result = 0;
		Field ALsize = null;
		Field DBALsize = null;
		
		Field[] alFields = ArrayList.class.getDeclaredFields();
	    Field[] dbalFields = DBArrayList.class.getDeclaredFields();
		
		for (int i=0;i<alFields.length;i++) {
			if ( alFields[i].getName().equalsIgnoreCase("size") ) {
				alFields[i].setAccessible(true);
				ALsize = alFields[i];
				break;
			}
		}
		
		for (int i=0;i<dbalFields.length;i++) {
			if ( dbalFields[i].getName().equalsIgnoreCase("v") ) {
				dbalFields[i].setAccessible(true);
				DBALsize = dbalFields[i];
				break;
			}
		}
		
		if (ALsize != null && DBALsize != null) {
			try {
				ArrayList l = (ArrayList) DBALsize.get(o);
				
				result = (Integer) ALsize.get(l);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} 
		}
		return result;
	}

}
