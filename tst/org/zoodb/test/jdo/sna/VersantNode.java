/*
 * @(#)VersantNode.java   1.0   Jul 27, 2011
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantNode.java 1998 2011-10-12 20:50:15Z D\michagro $
 */
package org.zoodb.test.jdo.sna;

import java.util.List;
import java.util.Map;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * A node of a graph.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class VersantNode extends ZooPCImpl {

   /**
    * An index of this node.
    */
   private final int id;

   /**
    * A label of this node.
    */
   private final String label;

   /**
    * All edges of this node.
    */
   private final VersantNodeEdges edges;

   /**
    * The row index of this node.
    */
   private final FastIdList<EdgePropertiesImpl> rowIndex;

   private int neighbourCount;

   /**
    * Constructs a default node.
    */
   public VersantNode() {
      super();
      this.id = -1;
      this.label = "";
      this.edges = null;
      this.rowIndex = null;
   }

   /**
    * Creates a new node with the given node index and node label.
    * 
    * @param id
    *           an id of the node.
    * @param label
    *           a label of the node.
    */
   public VersantNode(final int id, final String label) {
      super();
      this.id = id;
      this.label = label;
      this.edges = new VersantNodeEdges();
      this.rowIndex = new FastIdList<EdgePropertiesImpl>();
      this.neighbourCount = 0;
   }

   /**
    * @param id
    *           node ID
    * @param prop
    *           EdgeProperties for the given node
    */
   public void addEdgeProperty(final int id, final EdgePropertiesImpl prop) {
	   zooActivateWrite();
      this.rowIndex.put(id, prop);
   }

   public void addEdgeProperty(final Long id, final EdgePropertiesImpl prop) {
	   zooActivateWrite();
      this.rowIndex.put(id.intValue(), (EdgePropertiesImpl) prop);
   }

   /**
    * Returns a list of all edges incident to a node.
    * 
    * @return a list of all edges incident to a node.
    */
   public List<VersantEdge> getEdges() {
	   zooActivateRead();
      return this.edges.getEdges();
   }

   /**
    * Returns id of a node.
    * 
    * @return id of a node.
    */
   public int getBasicId() {
	   zooActivateRead();
      return this.id;
   }

   public Long getId() {
	   zooActivateRead();
      return Long.valueOf(this.id);
   }

   /**
    * Returns label of a node.
    * 
    * @return label of a node.
    */
   public String getLabel() {
	   zooActivateRead();
      return this.label;
   }

   /**
    * Inserts an egde to a list of edges incident to a node.
    * 
    * @param edge
    *            An edge to be inserted.
    */
   public void addEdge(final VersantEdge edge) {
	   zooActivateWrite();
      this.edges.addEdge(edge);
   }

   /**
    * Returns the row index.
    * 
    * @return row index.
    */
   public Map<Integer, EdgePropertiesImpl> getRowIndex() {
	   zooActivateRead();
      return this.rowIndex.asMap();
   }

   public void printNode() {
	   zooActivateRead();
      System.out.println("Node ID: " + this.id);
      System.out.println("Neigbours: ");
      for (int i = 0; i < this.rowIndex.size(); i++) {
    	  int currentKey = i + 1;
         System.out.println("Neigbor" + currentKey + " Shortest Path:"
               + this.rowIndex.get(currentKey).getDistance() + " Predecessor:"
               + this.rowIndex.get(currentKey).getPredecessor() + "Path Count:"
               + this.rowIndex.get(currentKey).getPathCount());
      }
   }

   public void setNeighbourCount() {
	   zooActivateWrite();
      this.neighbourCount++;

   }

   public int getNodeDegree() {
	   zooActivateRead();
      return this.neighbourCount;

   }

}
