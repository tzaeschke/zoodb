/*
 * @(#)VersantBuilder.java   1.0   Jul 27, 2011
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantBuilder.java 2004 2011-10-13 01:04:35Z D\michagro $
 */
package org.zoodb.test.sna;


/**
 * Builds a database graph from Pajek file.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class VersantBuilder {

   private VersantGraph graph;

   /**
    * Constructs a new VersantBuilder.
    * 
    * @param graph
    *           graph.
    */
   public VersantBuilder(final VersantGraph graph) {
      this.graph = graph;
   }

   /**
    * Returns graph.
    * 
    * @return graph.
    */
   public VersantGraph getGraph() {
      return this.graph;
   }

   /**
    * Handles and persists a node in an implementation specific way.
    * @param id id of a node.
    * @param label label of a node.
    */
   public void handleNode(final Long id, final String label) {
      final VersantNode node = new VersantNode(id.intValue(), label);
      this.addNode(node);
   }

   
   /**
    * Handles and persists an edge in an implementation specific way.
    * @param source source node id.
    * @param target target node id.
    * @param value weight of an edge.
    */
   public void handleEdge(final Long source, final Long target, final Double value) {

      final VersantNode srcNode = this.graph.getNode(source);
      final VersantNode trgNode = this.graph.getNode(target);

      final VersantEdge edge1 = new VersantEdge(srcNode, trgNode, value.floatValue());
      final VersantEdge edge2 = new VersantEdge(trgNode, srcNode, value.floatValue());

      this.addEdge(edge1);
      this.addEdge(edge2);
   }

   
   /**
    * Handles and persists an arc in an implementation specific way.
    * @param source source node id.
    * @param target target node id.
    * @param value weight of an arc.
    */
   public void handleArc(final Long source, final Long target, final Double value) {
      final VersantNode srcNode = this.graph.getNode(source);
      final VersantNode trgNode = this.graph.getNode(target);

      final VersantEdge edge = new VersantEdge(srcNode, trgNode, value.floatValue());
      this.addEdge(edge);
   }

   /**
    * Inserts a node into a graph.
    * 
    * @param node
    *           node to be inserted.
    */
   private void addNode(final VersantNode node) {
      this.graph.insertNode(node);
   }

   /**
    * Inserts an edge into a graph.
    * 
    * @param edge
    *           edge to be inserted.
    */
   private void addEdge(final VersantEdge edge) {
      this.graph.insertEdge(edge);
      final VersantNode srcNode = edge.getSource();
      srcNode.addEdge(edge);
   }

   /**
    * Computes predecessor matrix and inserts into a graph.
    */
   public void addPredecessorMatrix() {
      this.graph.floydWarshall();
   }

   
   /**
    * Closes the database.
    */
   public void close() {
      this.graph = null;
   }

}
