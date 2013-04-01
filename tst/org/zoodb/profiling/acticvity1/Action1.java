package org.zoodb.profiling.acticvity1;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.profiling.DBLPQueries;
import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.Conference;
import org.zoodb.profiling.model1.ConferenceSeries;
import org.zoodb.profiling.model1.Publication;
import org.zoodb.profiling.simulator.IAction;

public class Action1 implements IAction {

	public Object executeAction(PersistenceManager pm) {
		DBLPQueries queries = new DBLPQueries(pm, Author.class, ConferenceSeries.class);
		
		pm.currentTransaction().begin();
		List<ConferenceSeries> result = (List<ConferenceSeries>) queries.getConferenceSeriesByKey().execute("expdb");

//		List<ConferenceSeries> result = (List<ConferenceSeries>) queries.getConferenceSeriesByKey().execute("icoodb");
		
		//System.out.println("Conferences for series: 'icoodb': ");
		for (ConferenceSeries cs : result) {
			for (Conference c : cs.getConferences()) {
				//System.out.println(c.getIssue());
				Publication p = c.getPublications().get(0);
				//for (Publication p : c.getPublications()) {
					Author a = p.getTargetA().iterator().next();
					System.out.println("\t\t" + a.getName());
//					for (Author a : p.getTargetA()) {
//						System.out.println("\t\t" + a.getName());
//					}
					//System.out.println("\t" + p.getTitle());
				//}
				
			}
		}
		
		
				
		//simulate aggregation over publication.rating
		Query authorQuery = queries.getAuthorByNameQuery();
		List<Author> authorResults = (List<Author>) authorQuery.execute("Moira C. Norrie");
		
		for (Author a : authorResults) {
			//System.out.println(a.getName());
			a.getName();
			a.getSourceA().size();
			//System.out.println("Number of publications: " + a.getSourceA().size());
			
			for (Publication p : a.getSourceA()) {
				p.getRating();
				//System.out.println(p.getRating());
			}
		}
		
		
		
		pm.currentTransaction().commit();
		
		return null;
	}

}
