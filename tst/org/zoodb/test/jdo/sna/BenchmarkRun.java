/*
 * @(#)BenchmarkRun.java   1.0   Oct 12, 2011
 *
 * Copyright 2011-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: BenchmarkRun.java 1988 2011-10-12 19:32:08Z D\michagro $
 */
package org.zoodb.test.jdo.sna;

import java.util.HashMap;
import java.util.Map;

import org.zoodb.test.jdo.sna.VersantGraphBenchmark.Task;

/**
 * A run of the benchmark.
 * 
 * @author Michael Grossniklaus &lt;grossniklaus@cs.pdx.edu&gt;
 * @version 1.0
 */
public class BenchmarkRun {

   private final Task task;
   private final boolean success;
   private final long runtime;
   private final Map<Object, Object> params;

   public BenchmarkRun(final Task task, final boolean success, final long runtime) {
      this.task = task;
      this.success = success;
      this.runtime = runtime;
      this.params = new HashMap<Object, Object>();
   }

   public Task getTask() {
      return this.task;
   }

   public boolean isSuccess() {
      return this.success;
   }

   public long getRuntime() {
      return this.runtime;
   }

   public Map<Object, Object> getParameters() {
      return this.params;
   }

   @Override
   public String toString() {
      final StringBuffer result = new StringBuffer();
      result.append(BenchmarkRun.class.getSimpleName());
      result.append("[task=" + this.getTask() + ";success=" + this.isSuccess() + ";runtime="
            + this.getRuntime() + ";params=" + this.getParameters() + "]");
      return result.toString();
   }

}
