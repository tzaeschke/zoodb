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

import java.util.ArrayList;
import java.util.List;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * A node of a graph.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class VersantNodeEdges extends ZooPCImpl {

   /**
    * All edges of this node.
    */
   private final ArrayList<VersantEdge> edges;

   /**
    * Constructs a default node.
    */
   public VersantNodeEdges() {
      super();
      edges = new ArrayList<VersantEdge>();
   }

   /**
    * Returns a list of all edges incident to a node.
    * 
    * @return a list of all edges incident to a node.
    */
   public List<VersantEdge> getEdges() {
	   zooActivateRead();
      return this.edges;
   }

   /**
    * Inserts an egde to a list of edges incident to a node.
    * 
    * @param edge
    *            An edge to be inserted.
    */
   public void addEdge(final VersantEdge edge) {
	   zooActivateWrite();
      this.edges.add(edge);
   }

}
