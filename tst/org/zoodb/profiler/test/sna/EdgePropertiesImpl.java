/*
 * @(#)EdgePropertiesImpl.java   1.0   Jul 27, 2011
 *
 * Copyright 2010-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: EdgePropertiesImpl.java 1985 2011-10-12 16:40:10Z D\michagro $
 */
package org.zoodb.profiler.test.sna;


public class EdgePropertiesImpl {

   private float distance;
   private int predecessor;
   private short pathCount;

   public short getPathCount() {
      return this.pathCount;
   }

   public void setPathCount(final short pathCount) {
      this.pathCount = pathCount;
   }

   public void setPredecessor(final int predecessor) {
      this.predecessor = predecessor;
   }

   public int getPredecessor() {
      return this.predecessor;
   }

   public void setDistance(final float path) {
      this.distance = path;
   }

   public float getDistance() {
      return this.distance;
   }
}
