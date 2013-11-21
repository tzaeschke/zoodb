package ch.ethz.oserb.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.Check;
import net.sf.oval.ConstraintViolation;
import net.sf.oval.ValidatorFactory;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.constraint.OclConstraintCheck;
import net.sf.oval.exception.ConstraintsViolatedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

import ch.ethz.oserb.ConstraintManager;
import ch.ethz.oserb.ConstraintManagerFactory;
import ch.ethz.oserb.ConstraintManager.CouplingMode;
import ch.ethz.oserb.test.data.Student;

public class test_factory {
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
		File modelProviderClass = new File("resources/model/ch/ethz/oserb/test/data/ModelProviderClass.class");
		File oclConfig = new File("resources/constraints/test.ocl");
		ArrayList<OCLConfig> oclConfigs = new ArrayList<OCLConfig>();
		//oclConfigs.add(new OCLConfig(oclConfig, "oclConfigurer", 0));
		
		ValidatorFactory vf = new ValidatorFactory(modelProviderClass,oclConfigs);
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
	public void test() throws Exception {
		// define schemas
		cm.begin();
		PersistenceManager pm = cm.getPersistenceManager();
		ZooSchema.defineClass(pm, Student.class);
		cm.commit();
		
		// baseline
		try{
			cm.begin();
			Student alice = new Student(1,"Alice",19);
			cm.makePersistent(alice);
			cm.commit();
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 0);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// annotated ocl constraint failing
		try{
			cm.begin();
			Student bob = new Student(2,"Bob",17);
			cm.makePersistent(bob);
			cm.commit();
			fail("violation expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			// get constraint violations
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// removing all checks from reference validator -> no influence
		for(Check check:ConstraintManagerFactory.getChecks(Student.class)){
			ConstraintManagerFactory.removeChecks(Student.class, check);
		}
		try{
			cm.begin();
			Student eve = new Student(3,"Eve",17);
			cm.makePersistent(eve);
			cm.commit();
			fail("violations expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// adding a check at reference validator -> no influence
		OclConstraintCheck check = new OclConstraintCheck();
		check.setExpr("context Student inv: self.age>25");
		ConstraintManagerFactory.addChecks(Student.class, check);
		
		try{
			cm.begin();
			Student david = new Student(4,"David",17);
			cm.makePersistent(david);
			cm.commit();
			fail("violations expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
		
		// close constraint manager, acquiring new one
		cm.close();
		cm = ConstraintManagerFactory.getConstraintManager(CouplingMode.DEFERRED);
		
		try{
			cm.begin();
			Student adrien = new Student(5,"Adrien",24);
			cm.makePersistent(adrien);
			cm.commit();
			fail("violation expected! should never reach this code...");
		}catch (ConstraintsViolatedException e){
			// only newly added check age>25 failing
			assertEquals(e.getConstraintViolations().length, 1);
			cm.forceCommit();
		}finally{
			assert(cm.isClosed());
		}
	}

}
