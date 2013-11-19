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
import ch.ethz.oserb.test.data.Prof;

public class test_foreign {
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
		ZooSchema.defineClass(pm, Prof.class);
		ZooSchema.defineClass(pm, Departement.class);
		cm.disableAllProfiles();
		cm.enableProfile("foreign");
		cm.commit();
		
		// baseline
		try{
			cm.begin();
			Departement infk = new Departement(1,"D-INFK");
			cm.makePersistent(infk);
			Prof norrie = new Prof(1,"Norrie",1);
			cm.makePersistent(norrie);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			fail("baseline check");
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// missing corresponding primary
		try{
			cm.begin();
			Prof norrie = new Prof(2,"Weber",2);
			cm.makePersistent(norrie);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
	}

}
