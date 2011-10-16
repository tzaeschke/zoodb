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
package org.zoodb.test.sna;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * A node of a graph.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class VersantNode extends PersistenceCapableImpl {

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
   private List<VersantEdge> edges;

   /**
    * The row index of this node.
    */
   private LinkedHashMap<Integer, EdgePropertiesImpl> rowIndex;

   private int neighbourCount;

   /**
    * Constructs a default node.
    */
   public VersantNode() {
      super();
      this.id = -1;
      this.label = "";
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
      this.edges = new ArrayList<VersantEdge>();
      this.rowIndex = new LinkedHashMap<Integer, EdgePropertiesImpl>();
      this.neighbourCount = 0;
   }

   /**
    * @param id
    *           node ID
    * @param prop
    *           EdgeProperties for the given node
    */
   public void addEdgeProperty(final int id, final EdgePropertiesImpl prop) {
	   zooActivate();
	   jdoMakeDirty("");
      this.rowIndex.put(id, prop);
   }

   public void addEdgeProperty(final Long id, final EdgePropertiesImpl prop) {
	   zooActivate();
	   jdoMakeDirty("");
      this.rowIndex.put(id.intValue(), (EdgePropertiesImpl) prop);
   }

   /**
    * Returns a list of all edges incident to a node.
    * 
    * @return a list of all edges incident to a node.
    */
   public List<VersantEdge> getEdges() {
	   zooActivate();
      return this.edges;
   }

   /**
    * Returns id of a node.
    * 
    * @return id of a node.
    */
   public int getBasicId() {
	   zooActivate();
      return this.id;
   }

   public Long getId() {
	   zooActivate();
      return Long.valueOf(this.id);
   }

   /**
    * Returns label of a node.
    * 
    * @return label of a node.
    */
   public String getLabel() {
	   zooActivate();
      return this.label;
   }

   /**
    * Inserts an egde to a list of edges incident to a node.
    * 
    * @param edge
    *            An edge to be inserted.
    */
   public void addEdge(final VersantEdge edge) {
	   zooActivate();
	   jdoMakeDirty("");
      this.edges.add((VersantEdge) edge);
   }

   /**
    * Returns the row index.
    * 
    * @return row index.
    */
   public LinkedHashMap<Integer, EdgePropertiesImpl> getRowIndex() {
	   zooActivate();
      return this.rowIndex;
   }

   public void printNode() {
	   zooActivate();
      System.out.println("Node ID: " + this.id);
      System.out.println("Neigbours: ");
      for (final Integer currentKey : this.rowIndex.keySet()) {
         System.out.println("Neigbor" + currentKey + " Shortest Path:"
               + this.rowIndex.get(currentKey).getDistance() + " Predecessor:"
               + this.rowIndex.get(currentKey).getPredecessor() + "Path Count:"
               + this.rowIndex.get(currentKey).getPathCount());
      }
   }

   public void setNeighbourCount() {
	   zooActivate();
	   jdoMakeDirty("");
      this.neighbourCount++;

   }

   public int getNodeDegree() {
	   zooActivate();
      return this.neighbourCount;

   }

}
