/*
 * @(#)VersantRunner.java   1.0   Dec 8, 2010
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantRunner.java 1994 2011-10-12 19:43:24Z D\michagro $
 */
package org.zoodb.profiler.test.sna;

import java.io.IOException;

import org.zoodb.profiler.test.sna.GraphBenchmarkRunner;
import org.zoodb.profiler.test.sna.VersantGraphBenchmark;
import org.zoodb.profiler.test.sna.VersantRunner;

public final class VersantRunner {

//	   private static final String DB_NAME = "RandomRegularGraph-n1000-d20";
//	   private static final String DB_NAME = "test";
	   private static final String DB_NAME = "Erdos02";

   public static void main(final String[] args) {
      String dbName = VersantRunner.DB_NAME;
      if (args.length > 0) {
         dbName = args[0];
      }

      long start = System.currentTimeMillis();
      
      final VersantGraphBenchmark benchmark = new VersantGraphBenchmark(dbName);
      final GraphBenchmarkRunner runner = new GraphBenchmarkRunner(benchmark);
      try {
         runner.run();
      } catch (final IOException e) {
         e.printStackTrace();
      }
      
      long end = System.currentTimeMillis();
      System.out.println("Total: " + (end - start)/1000.);
   }

   private VersantRunner() {
      // hidden constructor
   }

}
