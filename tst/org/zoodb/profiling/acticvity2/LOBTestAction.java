package org.zoodb.profiling.acticvity2;

import java.util.List;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.profiling.model2.Author;
import org.zoodb.profiling.model2.Publication;
import org.zoodb.profiling.simulator.IAction;

public class LOBTestAction implements IAction {
	
	

	public Object executeAction(PersistenceManager pm) {
		
		int max = 10000;
		
		pm.currentTransaction().begin();
		
		Extent<Author> ae = pm.getExtent(Author.class);
		
		int i=0;
		for (Author a : ae) {
			i++;
			List<Publication> ps = a.getSourceA();
			
			if (i > max) {
				break;
			}
			
			for (Publication p : ps) {
				p.getRating();
				p.getKey();
			}
		}
		
		
		pm.currentTransaction().commit();
		return null;
	}
	
	

}
