/*
 * @(#)GraphBenchmarkRunner.java   1.0   Oct 12, 2011
 *
 * Copyright 2011-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: GraphBenchmarkRunner.java 1988 2011-10-12 19:32:08Z D\michagro $
 */
package org.zoodb.test.jdo.sna;

import java.io.IOException;

/**
 * Runs the graph benchmark.
 * 
 * @author Michael Grossniklaus &lt;grossniklaus@cs.pdx.edu&gt;
 * @version 1.0
 */
public class GraphBenchmarkRunner {

   private final VersantGraphBenchmark benchmark;

   public GraphBenchmarkRunner(final VersantGraphBenchmark benchmark) {
      this.benchmark = benchmark;
   }

   public void run() throws IOException {
      System.out.println("Running Graph Benchmark on database: " + this.benchmark.getName()
            + ".");

      // Data Loading.
      System.out.println("Data loading...");
      this.benchmark.load();

      // Query 1: Transitive Closure.
      System.out.println("Query 1: Transitive closure...");
      this.benchmark.queryOne();

      // Query 2: Node Degrees.
      System.out.println("Query 2: Node degrees...");
      this.benchmark.queryTwo();

      // Query 3: Connectedness.
      System.out.println("Query 3: Connectedness...");
      this.benchmark.queryThree();

      // Query 4: Shortest paths.
      System.out.println("Query 4: All shortest paths...");
      this.benchmark.queryFour();

      // Query 5: Degree Centrality.
      System.out.println("Query 5: All degree centralities...");
      this.benchmark.queryFive();

      // Query 6: Closeness Centrality.
      System.out.println("Query 6: All closeness centralities...");
      this.benchmark.querySix();

      // Query 7: Betweenness Centrality.
      System.out.println("Query 7: All betweenness centralities...");
      this.benchmark.querySeven();

      // Query 8: Bridges.
      System.out.println("Query 8: Find all bridges...");
      this.benchmark.queryEight();

      // Query 9: Average Path Length.
      System.out.println("Query 9: Graph diameter and average path length...");
      this.benchmark.queryNine();

      // Printing Statistics.
      for (final BenchmarkRun run : this.benchmark.getRuns()) {
         System.out.println(run);
      }

   }

}
