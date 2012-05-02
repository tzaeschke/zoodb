package org.zoodb.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.util.TestTools;

public class Test_081_SerailizationBugs {


	@Before
	public void before() {
		// nothing
	}


	/**
	 * Run after each test.
	 */
	@After
	public void after() {
		TestTools.closePM();
	}


	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
		TestTools.defineSchema(PersistentTwitData.class, PaperPage.class);
	}


	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}


	/**
	 * Test serialisation. 
	 */
	@Test
	public void testSerialization() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		PersistentTwitData map1 = new PersistentTwitData();
		PersistentTwitData map2 = new PersistentTwitData();
		Set<TwitInternal> set1 = new HashSet<TwitInternal>();
		set1.add(new TwitInternal());
		map2.twitsByPaperPage().put(
				new PaperPage("2", 1), set1);
		map2.twitsByPaperPage().put(
				new PaperPage("4444", 4), new HashSet<TwitInternal>());
		map2.twitsByPaperPage().put(
				new PaperPage("66666", 6), new HashSet<TwitInternal>());
		pm.makePersistent(map1);
		pm.makePersistent(map2);
		Object oid1 = pm.getObjectId(map1);
		Object oid2 = pm.getObjectId(map2);
		pm.currentTransaction().commit();
		TestTools.closePM();

		//check target
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//Check for content in target
		map1 = (PersistentTwitData) pm.getObjectById(oid1, true);
		map2 = (PersistentTwitData) pm.getObjectById(oid2, true);
		System.out.println("s1=" + map1.twitsByPaperPage().size());
		System.out.println("s2=" + map2.twitsByPaperPage().size());
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
}


class Tag {

}

class TwitInternal {
	//TODO not original code
	private Map<PaperPage, Integer> map = new HashMap<PaperPage, Integer>();
	private Set<PaperPage> set = new HashSet<PaperPage>();
	public TwitInternal() {
		map.put(new PaperPage("a", 10), 10);
		set.add(new PaperPage("c", 11));
	}
}


class ReceivedTwitImpl {

}

class PersistentTwitData extends PersistenceCapableImpl {

	private long nextId;
	private final Map<String, Tag> tagsByName;
	private final Map<Tag, Stack<TwitInternal>> twitsByTag;
	private final List<TwitInternal> twits;
	private final Map<PaperPage,Set<TwitInternal>> twitsByPaperPage;
	private final List<ReceivedTwitImpl> receivedTwits;


	public PersistentTwitData() {
		//	      this.tagsByName = new DBHashMap<String, TagImpl>();
		//	      this.textsByTag = new DBHashMap<TagImpl, Stack<String>>();
		this.tagsByName = new HashMap<String, Tag>();
		this.twitsByTag = new HashMap<Tag, Stack<TwitInternal>>();
		this.twits = new ArrayList<TwitInternal>();
		this.twitsByPaperPage = new HashMap<PaperPage, Set<TwitInternal>>();
		this.receivedTwits = new ArrayList<ReceivedTwitImpl>();
		this.nextId = 0;
	}

	public Map<String, Tag> tagsByName() {
		//	      this.zooActivateRead();
		this.zooActivateWrite();
		return this.tagsByName;
	}

	public Map<Tag, Stack<TwitInternal>> twitsByTag() {
		//	      this.zooActivateRead();
		this.zooActivateWrite();
		return this.twitsByTag;
	}

	public Map<PaperPage, Set<TwitInternal>> twitsByPaperPage() {
		this.zooActivateWrite();
		return this.twitsByPaperPage;
	}

	public List<TwitInternal> twits() {
		this.zooActivateWrite();
		return this.twits;
	}

	public long nextId() {
		this.zooActivateWrite();
		final long value = this.nextId;
		this.nextId++;
		return value;
	}

	public List<ReceivedTwitImpl> receivedTwits() {
		this.zooActivateWrite();
		return this.receivedTwits;
	}

}

class PaperPage extends PersistenceCapableImpl {

	private String documentID;
	private int pageNumber;

	private PaperPage() { 
		//private, for ZooDB
	}
	
	public PaperPage(final String documentID, final int pageNumber) {
		this.documentID = documentID;
		this.pageNumber = pageNumber;
	}

	public String getDocumentID() {
		this.zooActivateRead();
		return this.documentID;
	}

	public void setDocumentID(final String documentNumber) {
		this.zooActivateWrite();
		this.documentID = documentNumber;
	}

	public int getPageNumber() {
		this.zooActivateRead();
		return this.pageNumber;
	}

	public void setPageNumber(final int pageNumber) {
		this.zooActivateWrite();
		this.pageNumber = pageNumber;
	}


	@Override
	public int hashCode() {
		this.zooActivateRead();
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.documentID == null ? 0 : this.documentID.hashCode());
		result = prime * result + this.pageNumber;
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		this.zooActivateRead();
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PaperPage)) {
			return false;
		}
		final PaperPage other = (PaperPage) obj;
		if (this.documentID == null) {
			if (other.documentID != null) {
				return false;
			}
		} else if (!this.documentID.equals(other.documentID)) {
			return false;
		}
		if (this.pageNumber != other.pageNumber) {
			return false;
		}
		return true;
	}


}
