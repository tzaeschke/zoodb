package org.zoodb.profiling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.impl.ActivationArchive;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.AuthorContact;
import org.zoodb.profiling.model1.Conference;
import org.zoodb.profiling.model1.ConferenceSeries;
import org.zoodb.profiling.model1.Publication;
import org.zoodb.profiling.model1.Tags;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassMergeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.CollectionAggregationSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.DuplicateSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.LOBSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ReferenceShortcutSuggestion;

public class Test_010_Shortcut {

	private static List<Long> oids;

	
	@BeforeClass
	public static void beforeCLass() {
		TestTools.createDb();
		TestTools.defineSchema(ConferenceSeries.class, 
				Conference.class, 
				Publication.class, 
				Author.class,
				Tags.class,
				AuthorContact.class);
		
		oids = populate(10);
	}
	
	@Before
	public void before() {
		String tag = "myTag";
		ProfilingManager.getInstance().init(tag);
	}
	
	@After
	public void after() {
		TestTools.closePM();
		
		ProfilingManager.getInstance().finish();
		listActivations(ConferenceSeries.class);
		listActivations(Conference.class);
		listActivations(Publication.class);
		listActivations(Author.class);
		listActivations(Tags.class);
		
		listSuggestions(ProfilingManager.getInstance().internalGetSuggestions());

		ProfilingManager.getInstance().reset();
	}
	
	/**
	 * Produces: Shortcut, duplicate, LOB. 
	 */
	@Test
	public void test1() {
		for (int j = 0; j < 3; j++) {
		
			PersistenceManager pm = TestTools.openPM();
			for (int k = 0; k < 2; k++) {
				pm.currentTransaction().begin();
				
				//navigate
				for (int i = 0; i < 5; i++) {
					ConferenceSeries t = (ConferenceSeries) pm.getObjectById(oids.get(0));
					for (Conference c: t.getConferences()) {
						c.getIssue();
						c.getLocation();
						c.getYear();
						if (c.getPublications() != null && !c.getPublications().isEmpty()) {
							Publication p = c.getPublications().get(0);
							
							if (p.getTargetA() != null && !p.getTargetA().isEmpty()) {
								Author a = p.getTargetA().iterator().next();
								//this.getLogger().info("Keynote Speaker: " + a.getName());
								
								a.getName();
							}
						}
					}
				}
				
				pm.currentTransaction().commit();
			}
			TestTools.closePM();
		}
	}
	
	/**
	 * Produces: Aggregation, LOB. 
	 */
	@Test
	public void test2() {
		System.out.println("Test2");
		
		for (int j = 0; j < 3; j++) {
		
			PersistenceManager pm = TestTools.openPM();
			for (int k = 0; k < 2; k++) {
				pm.currentTransaction().begin();
				
				//navigate
				for (int i = 0; i < 5; i++) {
					ConferenceSeries t = (ConferenceSeries) pm.getObjectById(oids.get(0));
					for (Conference c: t.getConferences()) {
						c.getIssue();
						c.getLocation();
						c.getYear();
						for (Publication p: c.getPublications()) {
							p.getRating();
						}
						
					}
				}
				
				pm.currentTransaction().commit();
			}
			TestTools.closePM();
		}
		
	}
	
	/**
	 * Produces: Aggregation, duplicate, LOB, ClassMerge. 
	 */
	@Test
	public void test3() {
		System.out.println("Test3");
		
		for (int j = 0; j < 3; j++) {
		
			PersistenceManager pm = TestTools.openPM();
			for (int k = 0; k < 2; k++) {
				pm.currentTransaction().begin();
				
				//navigate
				for (int i = 0; i < 5; i++) {
					ConferenceSeries t = (ConferenceSeries) pm.getObjectById(oids.get(0));
					for (Conference c: t.getConferences()) {
						c.getIssue();
						c.getLocation();
						c.getYear();
						for (Publication p: c.getPublications()) {
							for (Author a: p.getTargetA()) {
								a.getName();
								a.getDetails().getEmail();
							}
						}
						
					}
				}
				
				pm.currentTransaction().commit();
			}
			TestTools.closePM();
		}
	}
	
	/**
	 * Produces: Aggregation, duplicate, LOB, ClassMerge. 
	 */
	@Test
	public void test4() {
		System.out.println("Test4");
		
		for (int j = 0; j < 3; j++) {
		
			PersistenceManager pm = TestTools.openPM();
			for (int k = 0; k < 2; k++) {
				pm.currentTransaction().begin();
				
				//navigate
				for (int i = 0; i < 5; i++) {
					ConferenceSeries t = (ConferenceSeries) pm.getObjectById(oids.get(0));
					for (Conference c: t.getConferences()) {
						for (Publication p: c.getPublications()) {
							p.getRating();
//							p.getCitationCount();
//							p.getDownloadCount();
							p.getTargetA();
						}
						
					}
				}
				
				for (int i = 0; i < 5; i++) {
					ConferenceSeries t = (ConferenceSeries) pm.getObjectById(oids.get(0));
					for (Conference c: t.getConferences()) {
						for (Publication p: c.getPublications()) {
							p.getKey();
							p.getTitle();
							p.getYear();
							p.getConference();
							p.getTargetT();
						}
						
					}
				}

				pm.currentTransaction().commit();
			}
			TestTools.closePM();
		}
	}
	
	private void listActivations(Class<?> cls) {
		ActivationArchive aArc = ProfilingManager.getInstance().getPathManager().getArchive(cls);
		if (aArc == null) {
			System.out.println("Class has no activations: " + cls.getName());
			return;
		}
		Iterator<AbstractActivation> it = aArc.getIterator();
		int n = 0;
		while (it.hasNext()) {
			it.next();
			//AbstractActivation aa = it.next(); 
			//System.out.println(cls.getSimpleName() + " " + aa.getClass().getSimpleName() + " -> " 
			//+ aa.getChildrenCount());
			n++;
		}
		System.out.println(cls.getSimpleName() + " has activations: " + n);
	}

	private void listSuggestions(Collection<AbstractSuggestion> suggestions) {
		int nS = 0;
		for (AbstractSuggestion s: suggestions) {
			String cg = "  c/g=" + s.getGain() + "/" + s.getCost();
			String cn = s.getClazzName();
			//System.out.println("name=" + s.getClass().getName() + "  --> " + s.getClazzName());
			if (s instanceof FieldRemovalSuggestion) {
				FieldRemovalSuggestion frs = (FieldRemovalSuggestion) s;
				System.out.println("RemovalF: " + cn + "." + frs.getFieldName() + cg); 
			} else if (s instanceof ClassRemovalSuggestion) {
				//ClassRemovalSuggestion frs = (ClassRemovalSuggestion) s;
				System.out.println("RemovalC: " + cn + cg); 
			} else if (s instanceof ReferenceShortcutSuggestion) {
				ReferenceShortcutSuggestion frs = (ReferenceShortcutSuggestion) s;
				System.out.println("Shortcut: " + cn + " to " + frs.getRefTarget() + cg);
			} else if (s instanceof CollectionAggregationSuggestion) {
				CollectionAggregationSuggestion frs = (CollectionAggregationSuggestion) s;
				System.out.println("Aggregation: " + cn + " < " + 
				frs.getAggregateeClass() + "." + frs.getAggregateeField() + cg);
			} else if (s instanceof DuplicateSuggestion) {
				DuplicateSuggestion frs = (DuplicateSuggestion) s;
				System.out.println("Duplicate: " + cn + " < " + 
				frs.getDuplicateeClass() + "." + frs.getDuplicateeField() + cg);
			} else if (s instanceof LOBSuggestion) {
				LOBSuggestion frs = (LOBSuggestion) s;
				System.out.println("LOB: " + cn + "." + frs.getFieldName() + ":" + 
				frs.getDetectionCount() + "/" + frs.getAvgLobSize() + cg);
			} else if (s instanceof ClassMergeSuggestion) {
				ClassMergeSuggestion frs = (ClassMergeSuggestion) s;
				System.out.println("ClassMerge: " + cn + "." + frs.getMasterClass() + "+" + 
				frs.getMergeeClass() + cg);
			} else {
				throw new IllegalArgumentException("Unknown: " + s.getClass().getName());
			}
			nS++;
		}
		System.out.println("Suggestions: " + nS);
	}
	
	private static List<Long> populate(int n) {
		Random R = new Random();
		
		List<Long> oids = new ArrayList<Long>(); 
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		List<Author> authors = new ArrayList<Author>();
		for (int i = 0; i < n*10; i++) {
			Author a1 = new Author();
			a1.setName("John Doe " + i);
			authors.add(a1);
			AuthorContact ac = new AuthorContact();
			ac.setEmail(a1.getName() + "@some.university.com");
			ac.setUniversity("Some university called " + i);
			a1.setDetails(ac);
		}
		
		List<Tags> tags = new ArrayList<Tags>();
		for (int l = 0; l < 10*n; l++) {
			Tags tag = new Tags();
			tag.setLabel("Tag" + l);
			tags.add(tag);
		}

		
		for (int i = 0; i < n; i++) {
			ConferenceSeries cs1 = new ConferenceSeries();
			for (int j = 0; j < 5; j++) {
				Conference c1 = new Conference();
				c1.setIssue("Issue"+j);
				c1.setLocation("Location"+j);
				c1.setSeries(cs1);
				c1.setYear(j+2000);
				cs1.addConferences(c1);
				for (int k = 0; k < 10; k++) {
					Publication p1 = new Publication();
					p1.setCitationCount(k+10);
					p1.setConference(c1);
					p1.setDownloadCount(k+5);
					p1.setRating(3);
					p1.setTitle("Title " + k);
					p1.setAbstract(DBLPUtils.getRandomString(1024));
					p1.setYear(c1.getYear());
					for (int l = 0; l < 3; l++) {
						p1.addTargetA(authors.get(l + R.nextInt(n*3)));
					}
					for (int l = 0; l < 3; l++) {
						p1.addTargetT(tags.get(l + R.nextInt(n*3)));
					}
					c1.addPublication(p1);
				}
			}
			pm.makePersistent(cs1);
			oids.add((Long) pm.getObjectId(cs1));
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		return oids;
	}
}
