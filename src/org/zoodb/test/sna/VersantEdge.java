/*
 * @(#)VersantEdge.java   1.0   Jul 27, 2011
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantEdge.java 1998 2011-10-12 20:50:15Z D\michagro $
 */
package org.zoodb.test.sna;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * An (directed) edge that links two nodes.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @version 1.0
 */
public class VersantEdge {// extends PersistenceCapableImpl {

   /**
    * A weight of this edge.
    */
   private final float value;

   /**
    * A source node of this edge.
    */
   private final VersantNode source;

   /**
    * A target node of this edge.
    */
   private final VersantNode target;

   private VersantEdge() {
	   super();
	   this.value = 0;
	   this.source = null;
	   this.target = null;
   }

   /**
    * Creates a new edge with a given source node, a target node and an edge weight.
    * 
    * @param source
    *           a source node of this edge.
    * @param target
    *           a target node of this edge.
    * @param value
    *           a weight of this edge.
    */
   public VersantEdge(final VersantNode source, final VersantNode target, final float value) {
      super();
      this.value = value;
      this.source = source;
      this.target = target;
   }

   /**
    * Returns weight of an edge.
    * 
    * @return weight of an edge.
    */
   public float getBasicValue() {
//	   zooActivate();
      return this.value;
   }

   //TODO callees
//   public double getValue() {
//	   zooActivate();
//      return this.value;
//   }

   /**
    * Returns source node of an edge
    * 
    * @return source node.
    */
   public VersantNode getSource() {
//	   zooActivate();
      return this.source;
   }

   /**
    * Returns target node of an edge.
    * 
    * @return target node.
    */
   public VersantNode getTarget() {
//	   zooActivate();
      return this.target;
   }

}
