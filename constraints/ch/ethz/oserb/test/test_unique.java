package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.ValidatorFactory;
import net.sf.oval.exception.ConstraintsViolatedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.ConstraintManagerFactory;
import ch.ethz.oserb.test.data.Departement;
import ch.ethz.oserb.test.data.Lecture;
import ch.ethz.oserb.test.data.Prof;

public class test_unique {
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
		ValidatorFactory vf = new ValidatorFactory();
		ConstraintManagerFactory.initialize(pmf,vf);
		cm = ConstraintManagerFactory.getConstraintManager(ConstraintManager.CouplingMode.DEFERRED);
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
		ZooSchema.defineClass(pm, Departement.class);
		ZooSchema.defineClass(pm, Prof.class);
		ZooSchema.defineClass(pm, Lecture.class);
		cm.disableAllProfiles();
		cm.enableProfile("unique");
		cm.commit();
		
		// baseline
		try{
			cm.begin();
			Departement infk = new Departement(1, "D-INFK");
			cm.makePersistent(infk);
			Prof norrie = new Prof(2,"Norrie",1);
			cm.makePersistent(norrie);
			Lecture is = new Lecture(3,"Information Systems", 8, 2);
			cm.makePersistent(is);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 0);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// non unique name
		try{
			cm.begin();
			Prof weber = new Prof(4,"Weber",1);
			cm.makePersistent(weber);
			Lecture is = new Lecture(5,"Information Systems", 8, 4);
			cm.makePersistent(is);
			cm.commit();
			fail("violation expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
	}
}
