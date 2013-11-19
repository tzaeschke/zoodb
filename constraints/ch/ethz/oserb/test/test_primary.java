package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.exception.ConstraintsViolatedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.ConstraintManagerFactory;
import ch.ethz.oserb.test.data.Student;

public class test_primary {
	private ConstraintManager cm;
	
	@Before
	public void setUp() throws Exception {
	    String dbName = "ExampleDB";
	    
        // remove database if it exists
        if (ZooHelper.dbExists(dbName)) {
            ZooHelper.removeDb(dbName);
        }

        // create database (by default, all database files will be created in %USER_HOME%/zoodb)
        ZooHelper.createDb(dbName);
        
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
			
		// get constraintManager
		ConstraintManagerFactory.initialize(pmf,ConstraintManager.CouplingMode.DEFERRED);
		cm = ConstraintManagerFactory.getConstraintManager();
	}

	@After
	public void tearDown() throws Exception {
        if (cm.currentTransaction().isActive()) {
            cm.currentTransaction().rollback();
        }
        cm.close();
        cm.getPersistenceManagerFactory().close();
	}
	
	@Test
	public void test() {
		// define schemas
		cm.begin();
		PersistenceManager pm = cm.getPersistenceManager();
		ZooSchema.defineClass(pm, Student.class);
		cm.disableAllProfiles();
		cm.enableProfile("primary");
		cm.commit();
		
		// baseline
		try{
			cm.begin();
			Student alice = new Student(0, "Alice");
			cm.makePersistent(alice);
			Student bob = new Student(1, "Bob");
			cm.makePersistent(bob);
			cm.commit();
		} catch (ConstraintsViolatedException e) {
			assertEquals(e.getConstraintViolations().length,0);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// primary key not null
		try{
			cm.begin();
			Student zulu = new Student();
			zulu.setName("Zulu");
			cm.makePersistent(zulu);
			cm.commit();
		} catch (ConstraintsViolatedException e) {
			assertEquals(e.getConstraintViolations().length,1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// primary key in db
		try{
			cm.begin();
			Student eve = new Student(1, "Eve");
			cm.makePersistent(eve);
			cm.commit();
		} catch (ConstraintsViolatedException e) {
			assertEquals(e.getConstraintViolations().length,1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// primary key in managed objects
		try{
			cm.begin();
			Student charlie = new Student(2, "Charlie");
			cm.makePersistent(charlie);
			Student david = new Student(2, "David");
			cm.makePersistent(david);
			cm.commit();
		} catch (ConstraintsViolatedException e) {
			assertEquals(e.getConstraintViolations().length,2);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
	}
}
