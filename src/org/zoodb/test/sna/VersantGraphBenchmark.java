/*
 * @(#)VersantGraphBenchmark.java   1.0   Oct 12, 2011
 *
 * Copyright 2011-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantGraphBenchmark.java 2019 2011-10-14 11:28:40Z D\sleone $
 */
package org.zoodb.test.sna;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Graph benchmark.
 * 
 * @author Michael Grossniklaus &lt;grossniklaus@cs.pdx.edu&gt;
 * @version 1.0
 */
public class VersantGraphBenchmark {

	public enum Task {
		LOAD, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9
	};

	private static final int RUNS = 1; //TODO
	private static final int NODES = 1000;

	private final DBPopulate db;
	private final String dbName;
	private final List<BenchmarkRun> runs;

	public VersantGraphBenchmark(final String dbName) {
		this.dbName = dbName;
		this.runs = new LinkedList<BenchmarkRun>();
		this.db = new DBPopulate(dbName);
		this.db.close();

	}

	public String getName() {
		return this.dbName;
	}

	public List<BenchmarkRun> getRuns() {
		return this.runs;
	}

	/**
	 * Loading of the data set.
	 */
	 public void load() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();
			 this.db.deleteAllFromDB();

			 final long start = System.currentTimeMillis();
			 this.db.dbPopulate(this.dbName + ".net");
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.LOAD + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.LOAD + ": " + this.average(times));
	 }

	 /**
	  * Query 1: Transitive Closure.
	  */
	 public void queryOne() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 // Reset database
			 this.db.open();
			 this.db.deleteAllFromDB();
			 this.db.dbPopulate(this.dbName + ".net");
			 DBPopulate.commit();
			 this.db.close();

			 // Transitive closure
			 this.db.open();

			 final long start = System.currentTimeMillis();
			 this.db.makeTransitiveClosure();
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q1 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q1 + ": " + this.average(times));
	 }

	 /**
	  * Query 2: Node Degrees.
	  */
	 public void queryTwo() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();
			 final List<Integer> ids = this.generateNodeIds(graph);

			 final long start = System.currentTimeMillis();
			 for (final Integer id : ids) {
				 graph.nodeDegree(id);
			 }
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q2 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q2 + ": " + this.average(times));
	 }

	 /**
	  * Query 3: Connectedness
	  */
	 public void queryThree() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();
			 final List<Integer> ids = this.generateNodeIds(graph);

			 // TODO
			 final long start = System.currentTimeMillis();
			 for (final Integer source : ids) {
				 for (final Integer target : ids) {
					 graph.connected(source, target);
				 }
			 }
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q3 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q3 + ": " + this.average(times));
	 }

	 /**
	  * Query 4: Shortest paths
	  */
	 public void queryFour() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();
			 final List<Integer> ids = this.generateNodeIds(graph);

			 // TODO
			 final long start = System.currentTimeMillis();
			 for (final Integer source : ids) {
				 for (final Integer target : ids) {
					 graph.shortestPath(source, target);
				 }
			 }
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q4 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q4 + ": " + this.average(times));
	 }

	 /**
	  * Query 5: Degree centralities
	  */
	 public void queryFive() {
		 // TODO
	 }

	 /**
	  * Query 6: Closeness centralities
	  */
	 public void querySix() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();
			 final List<Integer> ids = this.generateNodeIds(graph);

			 final long start = System.currentTimeMillis();
			 for (final Integer id : ids) {
				 graph.closenessCentrality(id);
			 }
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q6 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q6 + ": " + this.average(times));
	 }

	 /**
	  * Query 7: Betweenness centralities
	  */
	 public void querySeven() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();
			 final List<Integer> ids = this.generateNodeIds(graph);

			 final long start = System.currentTimeMillis();
			 int n = 1;
			 for (final Integer id : ids) {
//			     if (n%10 == 0) {
			         System.out.println("Q7-commit: " + n + "/" + ids.size());
//			         DBPopulate.cleanCache();
//			     }
			     n++;
				 graph.betweennessCentralityUndirected(id);
			 }
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q7 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q7 + ": " + this.average(times));
	 }

	 /**
	  * Query 8: Bridges
	  */
	 public void queryEight() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();

			 final long start = System.currentTimeMillis();
			 graph.findBridges();
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q8 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q8 + ": " + this.average(times));
	 }

	 /**
	  * Query 9: Average Path Length and Diameter
	  */
	 public void queryNine() {
		 final long[] times = new long[VersantGraphBenchmark.RUNS];
		 for (int i = 0; i < VersantGraphBenchmark.RUNS; i++) {
			 this.db.open();

			 final VersantGraph graph = this.db.getGraph();

			 final long start = System.currentTimeMillis();
			 graph.averagePathAndDiameter();
			 final long stop = System.currentTimeMillis();

			 DBPopulate.commit();
			 this.db.close();
			 times[i] = stop - start;
			 System.out.println(Task.Q9 + "[" + i + "]: " + times[i]);
		 }
		 System.out.println(Task.Q9 + ": " + this.average(times));
	 }

	 private double average(final long[] times) {
		 Arrays.sort(times);
		 double sum = 0;
		 for (int i = 1; i < times.length - 1; i++) {
			 sum += times[i];
		 }
		 return sum / (times.length - 2);
	 }

	 private List<Integer> generateNodeIds(final VersantGraph graph) {
		 final List<Integer> result = new ArrayList<Integer>();
		 final int size = graph.getNodeMap().size();
		 final double inc = (double) size / VersantGraphBenchmark.NODES;
		 if (inc < 1) {
			 for (int i = 1; i <= size; i++) {
				 result.add(Integer.valueOf(i));
			 }
		 } else {
			 for (double sum = 1; sum <= size; sum += inc) {
				 result.add(Integer.valueOf((int) Math.floor(sum)));
			 }
		 }
		 System.out.println("Testing " + result.size() + " nodes.");
		 return result;
	 }
}
