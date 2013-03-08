/*
 * @(#)DBPopulate.java   1.0   Jul 27, 2011
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: DBPopulate.java 2019 2011-10-14 11:28:40Z D\sleone $
 */
package org.zoodb.test.sna;

import java.io.IOException;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooHelper;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;

/**
 * Database population class.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class DBPopulate {

   private VersantGraph graph;
   private final String dbName;
   private static PersistenceManager session;
   private static PersistenceManagerFactory pmf;

   /**
    * Constructs object with new session to Versant database and reads database
    * into graph object.
    */
   public DBPopulate(final String dbName) {

      this.dbName = dbName;
      if (DBPopulate.session == null) {
         this.open();
      }
   }

   public static void commit() {
	      DBPopulate.session.currentTransaction().commit();
	      DBPopulate.session.currentTransaction().begin();
   }

   public static void cleanCache() {
//	   commit();
//      DBPopulate.session.evictAll(false, VersantEdge.class);
//      DBPopulate.session.evictAll(false, VersantNode.class);
      DBPopulate.session.evictAll();
   }

   public void open() {

      try {
          final ZooJdoProperties property = new ZooJdoProperties(this.dbName);
    	  if (DBPopulate.pmf == null) {
		      if (ZooHelper.getDataStoreManager().dbExists(this.dbName)) {
		          ZooHelper.getDataStoreManager().removeDb(this.dbName);
		      }
		      ZooHelper.getDataStoreManager().createDb(this.dbName);

		      DBPopulate.pmf = JDOHelper.getPersistenceManagerFactory(property);
		      DBPopulate.session = pmf.getPersistenceManager();
		      DBPopulate.session.currentTransaction().begin();
		      ZooSchema.defineClass(session, VersantGraph.class);
		      ZooSchema.defineClass(session, VersantNodeEdges.class);
//              ZooSchema.create(session, VersantEdge.class);
		      ZooSchema.defineClass(session, VersantNode.class);
	         commit();
	         close();
    	  }
    	  
    	  DBPopulate.session = pmf.getPersistenceManager();
    	  DBPopulate.session.currentTransaction().begin();
    	  this.graph = this.getGraphFromDB();

      } catch (final Exception e) {
         e.printStackTrace();
         System.exit(1);
      }

   }

   public void close() {
      DBPopulate.session.currentTransaction().rollback();
      DBPopulate.session.close();
      DBPopulate.session = null;
      this.graph = null;
   }

   /**
    * Populates graph with information from pajek file.
    * 
    * @param pajekFile
    *           path to pajek file.
    */
   public void dbPopulate(final String pajekFile) {

      final VersantGraph graph = new VersantGraph(this.dbName);
      DBPopulate.session.makePersistent(graph);
      final VersantBuilder builder = new VersantBuilder(graph);
      final PajekParser parser = new PajekParser(builder);

      try {
         parser.parse(pajekFile);
      } catch (final IOException e) {
         e.printStackTrace();
      }

      commit();
   }

   /**
    * Makes transitive closure matrix and persists to database.
    */
   public void makeTransitiveClosure() {

      final VersantBuilder obj = new VersantBuilder(getGraph());
      obj.addPredecessorMatrix();
      commit();
   }

   /**
    * Returns graph object from Versant database with default graph name.
    * 
    * @return graph.
    */
   public VersantGraph getGraphFromDB() {
      VersantGraph graph = null;
      try {
    	  Extent<VersantGraph> ext = session.getExtent(VersantGraph.class);
    	  Iterator<VersantGraph> it = ext.iterator();
    	  if (it.hasNext()) {
    		  graph = it.next();
    	  } else {
    		  graph = null;
    	  }
    	  ext.closeAll();
      } catch (final Exception e) {
         // e.printStackTrace();
      }
      return graph;
   }

   /**
    * Deletes all objects in Versant database.
    */
   public void deleteAllFromDB() {
//	   Extent extG = session.getExtent(VersantGraph.class);
//	   session.newQuery(extG).deletePersistentAll();
//	   Extent extN = session.getExtent(VersantNode.class);
//	   session.newQuery(extN).deletePersistentAll();
//	   Extent extE = session.getExtent(VersantEdge.class);
//	   session.newQuery(extE).deletePersistentAll();
	   ZooSchema.locateClass(session, VersantGraph.class).dropInstances();
	   ZooSchema.locateClass(session, VersantNode.class).dropInstances();
//	   ZooSchema.locate(session, VersantEdge.class).dropInstances();
	   ZooSchema.locateClass(session, VersantNodeEdges.class).dropInstances();

      this.graph = null;
      DBPopulate.commit();
   }

   /**
    * Returns graph object.
    * 
    * @return graph.
    */
   public VersantGraph getGraph() {
      if (this.graph == null) {
         this.graph = this.getGraphFromDB();
      }
      //System.out.println("graph = " + graph);
      return this.graph;
   }
}
