package org.zoodb.profiling.api;

import java.lang.reflect.Field;

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
				int size = ((DBArrayList) o).size();
				((CollectionActivation) a).setSize(size);
				
				//Field f = o.getClass().getDeclaredField("v");
				//((CollectionActivation) a).setType(type);
			}

		} else {
			// which type of activation
			a = new PCActivation();
		}
		
		a.setClazz(o.getClass());
		a.setBytes(o.getTotalReadEffort());
		a.setOid(o.jdoZooGetOid());
		a.setTrx(ProfilingManager.getInstance().getCurrentTrxId());
		
		/*
		 * If predecessor ist not null
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

}
