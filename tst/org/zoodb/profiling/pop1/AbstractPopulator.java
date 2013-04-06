package org.zoodb.profiling.pop1;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.profiling.DBLPUtils;
import org.zoodb.profiling.model1.Publication;

public class AbstractPopulator {
	
	public static void main(String[] args) {
		PersistenceManager pm = openDB(args[0]);
		
		pm.currentTransaction().begin();
		boolean started = true;
		Extent<Publication> publications = pm.getExtent(Publication.class);
		//use the following extent when populating for the optimized model
		//Extent<PublicationAbstract> publications = pm.getExtent(PublicationAbstract.class);
		
		int i=0;
		
		for (Publication p : publications) {
			if (i==0 && !started) {
				pm.currentTransaction().begin();
			}
			String sAbstract = DBLPUtils.getRandomString(); 
			//un-optimized model
			p.setAbstract(sAbstract);
			//end un-optimized model
			
			//optimized model
			//PublicationAbstract pa = new PublicationAbstract();
			//p.getpAbstract().setAbstract(sAbstract);
			//p.setAbstract(sAbstract);
			//end optimized model
			
			i++;
			
			if (i==20000) {
				pm.currentTransaction().commit();
				started = false;
				i=0;
			}
		}
		if (i<20000) pm.currentTransaction().commit();
		System.out.println("before closing db");
		
		closeDB(pm);
		
	}
	
    private static PersistenceManager openDB(String dbName) {
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        return pm;
    }
    
    private static void closeDB(PersistenceManager pm) {
        if (pm.currentTransaction().isActive()) {
            pm.currentTransaction().rollback();
        }
        pm.close();
        pm.getPersistenceManagerFactory().close();
    }

}
