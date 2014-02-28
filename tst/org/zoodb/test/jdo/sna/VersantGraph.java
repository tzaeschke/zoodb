/*
 * @(#)VersantGraph.java   1.0   Jul 27, 2011
 *
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: VersantGraph.java 2019 2011-10-14 11:28:40Z D\sleone $
 */
package org.zoodb.test.jdo.sna;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * Represents a graph consisting of a set of nodes and a set of edges and
 * redundant information about Predecessor matrix via row indices.
 * 
 * @author Ilija Bogunovic &lt;ilijab@student.ethz.ch&gt;
 * @author Darijan Jankovic &lt;jdarijan@student.ethz.ch&gt;
 * @author Stefania leone &lt;leone@inf.ethz.ch&gt;
 * @version 1.0
 */
public class VersantGraph extends ZooPCImpl {

   /**
    * Constant for the predecessor matrix
    */
   public static final int NEIGHBOUR_NODE = -1;
   /**
    * Constant for the predecessor matrix
    */
   public static final int UNREACHABLE_NODE = -2;
   /**
    * Constant for the predecessor matrix
    */
   public static final int DISTANCETOSELF = 0;

   /**
    * Vertex states for bridge-finding algorithm
    */
   enum VertexState {
      visited, notVisited, progress
   };

   /**
    * The name of this graph.
    */
   private final String name;

   /**
    * Map of nodes objects that form this graph.
    */
   private final FastIdList<VersantNode> nodes;

   /**
    * A set of edge objects that connect all the nodes inside this graph.
    */
   private final HashSet<VersantEdge> edges;

   /**
    * private constructor for JDO.
    */
   @SuppressWarnings("unused")
   private VersantGraph() {
      this.name = null;
      this.nodes = null;
      this.edges = null;
   }

   /**
    * Constructs a default Graph object with default name and empty collections.
    */
   public VersantGraph(final String name) {
      this.name = name;
      this.nodes = new FastIdList<VersantNode>();
      this.edges = new HashSet<VersantEdge>();
   }

   /**
    * Returns the name of a graph.
    * 
    * @return name of the graph.
    */
   public String getName() {
	   zooActivateRead();
      return this.name;
   }

   public Collection<VersantNode> getNodes() {
	   zooActivateRead();
      return null;
   }

   /**
    * Returns node with id <code>nodeId</code>.
    * 
    * @param id
    *           id of a node in the graph.
    * @return node of a graph.
    */
   //TODO callee (LONG)
   public VersantNode getNode(final Long id) {
	   zooActivateRead();
      return this.getNodeInternal(id.intValue());
   }

   private VersantNode getNodeInternal(final int id) {
      VersantNode node = null;
      try {
         node = this.nodes.get(id);
      } catch (final Exception ex) {
         System.out.println("Node with " + id + " id does not exist.");
      }
      return node;
   }

   /**
    * Returns an edge between a source node with id <code>sourceId</code> and a
    * target node with id <code>targetId</code>.
    * 
    * @param sourceId
    *           id of a source node in the graph.
    * @param targetId
    *           id of a target node in the graph.
    * @return an edge between given nodes if exists.
    */
   public VersantEdge getEdge(final Long sourceId, final Long targetId) {
	   zooActivateRead();
      return this.getEdgeInternal(sourceId.intValue(), targetId.intValue());
   }

   private VersantEdge getEdgeInternal(final int sourceNodeIndex,
         final int targetNodeIndex) {
      for (final VersantEdge e : this.getEdges()) {
         if (e.getSource().getBasicId() == sourceNodeIndex
               && e.getTarget().getBasicId() == targetNodeIndex) {
            return e;
         }
      }
      return null;
   }

   /**
    * Returns a row index with id <code>rowIndexId</code>.
    * 
    * @param index
    *           id of a row index in the predecessor matrix.
    * @return row index of the predecessor matrix.
    */
   public Map<Integer, EdgePropertiesImpl> getRowIndex(final int index) {
	   zooActivateRead();
      return this.nodes.get(index).getRowIndex();
   }

   /**
    * Returns a map containing all nodes.
    * 
    * @return a map containing all nodes.
    */
   public Map<Integer, VersantNode> getNodeMap() {
	   zooActivateRead();
      return this.nodes.asMap();
   }

   /**
    * Returns a set containing all edges.
    * 
    * @return a set containing all edges.
    */
   public HashSet<VersantEdge> getEdges() {
	   zooActivateRead();
      return this.edges;
   }

   /**
    * Returns a map containing all row indices.
    * 
    * @return a map containing all row indices.
    */
   // @Override
   // public Map<Object, RowIndex> getRowIndices() {
   // return this.rowIndices;
   // }

   /**
    * Inserts the specified element in the map of all nodes.
    * 
    * @param node
    *           node to be inserted.
    */
   public void insertNode(final VersantNode node) {
	   zooActivateWrite();
      this.nodes.put(((VersantNode) node).getBasicId(), (VersantNode) node);
   }

   /**
    * Inserts the specified element in the set of all edges.
    * 
    * @param edge
    *           edge to be inserted.
    */
   public void insertEdge(final VersantEdge edge) {
	   zooActivateWrite();
      this.edges.add((VersantEdge) edge);
   }

   /**
    * Removes the specified element from the set of all edges.
    * 
    * @param edge
    *           edge to be removed.
    */
   public void removeEdge(final VersantEdge edge) {
	   zooActivateWrite();
      this.edges.remove(edge);
   }

   /**
    * Returns the degree of the given node. The degree is defined as the number
    * of ties of a node to other nodes in the graph.
    * 
    * @param nodeId
    *           id of a node in the graph.
    * @return degree of node with id <code>nodeId</code>.
    */
   public int nodeDegree(final int nodeId) {
	   zooActivateRead();
      return this.getNodeInternal(nodeId).getNodeDegree();
   }

   /**
    * Checks whether there is a path in the given graph between the node with id
    * <code>sourceNodeId</code> and the node with id <code>targetNodeId</code>.
    * 
    * @param sourceNodeId
    *           id of the source node.
    * @param targetNodeId
    *           id of the target node.
    * @return <code>true</code> if the given nodes are connected,
    *         <code>false</code> otherwise.
    */
   public boolean connected(final int sourceNodeId, final int targetNodeId) {
	   zooActivateRead();

      final VersantNode node = this.nodes.get(sourceNodeId);

      for (final VersantEdge edge : node.getEdges()) {
         if (edge.getTarget().getBasicId() == targetNodeId) {
            return true;
         }
      }

      return false;
   }

   public double betweennessCentralityUndirected(final int nodeId) {
	   zooActivateRead();
	   return FloydWarshall.betweennessCentralityUndirected(nodeId, this.nodes.asMap());
//      double sum = 0.0;
//
//      for (int i = 1; i < this.nodes.size() + 1; i++) {
//         for (int j = i + 1; j < this.nodes.size() + 1; j++) {
//
//            if (i != nodeId && j != nodeId) {
//               final VersantNode n1 = this.nodes.get(i);
//
//               final EdgePropertiesImpl ep1 = n1.getRowIndex().get(j);
//               final EdgePropertiesImpl ep2 = n1.getRowIndex().get(nodeId);
//
//               if (ep1.getDistance() > ep2.getDistance()) {
//                  final VersantNode n3 = this.nodes.get(nodeId);
//                  final EdgePropertiesImpl ep3 = n3.getRowIndex().get(j);
//
//                  if (ep2.getDistance() + ep3.getDistance() == ep1
//                        .getDistance()) {
//                     sum += (double) (ep2.getPathCount() * ep3.getPathCount())
//                           / ep1.getPathCount();
//                  }
//               }
//            }
//         }
//      }
//      return 2 * sum;
   }

   public double betweennessCentralityDirected(final int nodeId) {
	   zooActivateRead();
	   return FloydWarshall.betweennessCentralityDirected(nodeId, this.nodes.asMap());
//      Double sum = 0.0;
//
//      for (int i = 1; i < this.nodes.size() + 1; i++) {
//         for (int j = 1; j < this.nodes.size() + 1; j++) {
//
//            if (i != j && i != nodeId && j != nodeId) {
//               final VersantNode n1 = this.nodes.get(i);
//
//               final EdgePropertiesImpl ep1 = n1.getRowIndex().get(j);
//               final EdgePropertiesImpl ep2 = n1.getRowIndex().get(nodeId);
//
//               if (ep1.getDistance() > ep2.getDistance()) {
//                  final VersantNode n3 = this.nodes.get(nodeId);
//                  final EdgePropertiesImpl ep3 = n3.getRowIndex().get(j);
//
//                  if (ep2.getDistance() + ep3.getDistance() == ep1
//                        .getDistance()) {
//                     sum += (double) (ep2.getPathCount() * ep3.getPathCount())
//                           / ep1.getPathCount();
//                  }
//               }
//            }
//         }
//      }
//      return sum;
   }

   /**
    * Computes closeness centrality on a unweighted graph for the node with id
    * <code>nodeId</code>.
    * 
    * @param nodeId
    *           id of a node in the graph.
    * @return closeness centrality of the given node.
    */
   public double closenessCentrality(final int nodeId) {
	   zooActivateRead();

      final VersantNode source = this.getNodeInternal(nodeId);
      final Map<Integer, EdgePropertiesImpl> props = source.getRowIndex();

      double sum = 0.0;
      double d = 0.0;
      boolean removeMe = false;
      for (final EdgePropertiesImpl current : props.values()) {
    	  if (current == null) {
    		  if (removeMe == true) {
    			  throw new RuntimeException("Only the first element should be null!");
    		  }
    		  removeMe = true;
    		  continue;
    	  }
         d = current.getDistance();
         if (d != VersantGraph.UNREACHABLE_NODE) {
            sum += d;
         }
      }

      return 1 / sum;
   }

   /**
    * Computes and returns the average path and diameter of the given graph.
    * 
    * @return average path and diameter.
    */
   public double averagePathAndDiameter() {
	   zooActivateRead();

      double current = 0.0;
      double d = 0.0;
      for (int i = 1; i < this.nodes.size() + 1; i++) {

         final VersantNode srcNode = this.nodes.get(i);

         boolean removeMe = false;
         for (final EdgePropertiesImpl props : srcNode.getRowIndex().values()) {
          	  if (props == null) {
        		  if (removeMe == true) {
        			  throw new RuntimeException("Only the first element should be null!");
        		  }
        		  removeMe = true;
        		  continue;
        	  }
            current = props.getDistance();
            if (d != VersantGraph.UNREACHABLE_NODE) {
               if (current > d) {
                  d = current;
               }
            }
         }
      }

      return d;

   }

   /**
    * Finds and returns all ids of nodes that are bridges in the given graph.
    * 
    * @return list of all edges representing bridges.
    */
   public ArrayList<VersantEdge> findBridges() {
      final FindBridgesAlgorithm fba = new FindBridgesAlgorithm();
      return fba.findBridges();
   }

   /**
    * Class designed exclusively to find bridges for the given graph.
    */
   class FindBridgesAlgorithm {

      private final ArrayList<ArrayList<Integer>> dfsTree = new ArrayList<ArrayList<Integer>>();
      private final ArrayList<HashMap<Integer, ArrayList<Integer>>> backEdges = new ArrayList<HashMap<Integer, ArrayList<Integer>>>();
      private final ArrayList<Integer> dfsTreeTemp = new ArrayList<Integer>();
      private final HashMap<Integer, ArrayList<Integer>> backEdgesTemp = new HashMap<Integer, ArrayList<Integer>>();

      /**
       * Bridge finding algorithm.
       * 
       * @return List of edges which are considered bridges.
       */
      public ArrayList<VersantEdge> findBridges() {
    	  zooActivateRead();
         final ArrayList<VersantEdge> bridges = new ArrayList<VersantEdge>();
         final ArrayList<Integer> nd = new ArrayList<Integer>();
         final ArrayList<Integer> l = new ArrayList<Integer>();
         final ArrayList<Integer> h = new ArrayList<Integer>();

         this.dfs();

         int lt;
         int ht;
         int noDesc;

         final int nodesCount = VersantGraph.this.nodes.size();
         for (int i = 0; i < nodesCount + 1; i++) {
            nd.add(0);
            l.add(0);
            h.add(0);
         }

         for (int j = 0; j < this.dfsTree.size(); j++) {

            final ArrayList<Integer> nodesTree = this.dfsTree.get(j);
            final HashMap<Integer, ArrayList<Integer>> backEdges = this.backEdges.get(j);

            for (final Integer i : nodesTree) {
               final VersantNode nI = VersantGraph.this.getNodeInternal(i);

               noDesc = 1;
               lt = i;
               ht = i;
               int u;
               int lv;
               int hv;
               int p;
               for (final VersantEdge neighbourEdge : nI.getEdges()) {
                  u = neighbourEdge.getTarget().getBasicId();
                  lv = l.get(u);
                  hv = h.get(u);
                  if (u > i) {
                     if (lt > lv) {
                        lt = lv;
                     }
                     if (ht < hv) {
                        ht = hv;
                     }
                     noDesc += nd.get(u);

                     //TODO i i.o. Integer.valueOf ??
                  } else if (backEdges.get(i) != null
                        && backEdges.get(i).contains(u)) {
                     if (lt > u) {
                        lt = u;
                     }
                     if (ht < u) {
                        ht = u;
                     }
                  }
               }
               l.set(i, lt);
               h.set(i, ht);
               nd.set(i, noDesc);

               for (final VersantEdge currentEdge : nI.getEdges()) {
                  p = currentEdge.getTarget().getBasicId();
                  if (p > i) {
                     if (l.get(p) >= p && h.get(p) < p + nd.get(p)) {
                        bridges.add(currentEdge);
                     }
                  }
               }
            }
         }
         return bridges;
      }

      /**
       * Depth-first search algorithm which makes a spanning tree of a graph.
       */
      private void dfs() {
    	  zooActivateRead();
         final int graphSize = VersantGraph.this.nodes.size();
         final VertexState[] state = new VertexState[graphSize];

         for (int i = 0; i < graphSize; i++) {
            state[i] = VertexState.notVisited;
         }

         for (int i = 1; i <= graphSize; i++) {
            if (state[i - 1] == VertexState.notVisited) {

               final VersantNode srcNode = VersantGraph.this.getNodeInternal(i);
               this.runDFS(srcNode.getBasicId(), state);

               this.dfsTree.add(new ArrayList<Integer>(this.dfsTreeTemp));
               this.backEdges.add(new HashMap<Integer, ArrayList<Integer>>(
                     this.backEdgesTemp));
               this.dfsTreeTemp.clear();
               this.backEdgesTemp.clear();
            }
         }
      }

      /**
       * Single step in dfs-spanning tree algorithm.
       * 
       * @param u
       *           current node id.
       * @param state
       *           state of all nodes.
       */
      private void runDFS(final int u, final VertexState[] state) {
         final VersantNode node = VersantGraph.this.getNodeInternal(u);

         state[u - 1] = VertexState.progress;
         for (final VersantEdge neighbourEdge : node.getEdges()) {
            final VersantNode neighbourNode = neighbourEdge.getTarget();
            final int i = neighbourNode.getBasicId();

            if (state[i - 1] == VertexState.notVisited) {
               this.runDFS(i, state);
            } else {
            	ArrayList<Integer> backEdgesNodeList = this.backEdgesTemp.get(i);
               if (backEdgesNodeList == null) {
                  backEdgesNodeList = new ArrayList<Integer>();
               }
               backEdgesNodeList.add(u);
               this.backEdgesTemp.put(i, backEdgesNodeList);
            }

         }
         state[u - 1] = VertexState.visited;
         this.dfsTreeTemp.add(u);
      }
   }

   /**
    * Computes all shortest paths among all nodes in network.
    * 
    * Results in predecessor matrix(?).
    */
   public void floydWarshall() {
	   zooActivateRead();

      /**
       * The weight matrix.
       */
      float[][] d;

      /**
       * The predecessor matrix.
       */
      short[][] p;

      /**
       * The path count matrix.
       */
      short[][] c;

      final int nodesCount = VersantGraph.this.nodes.size();

      d = new float[nodesCount][nodesCount];
      p = new short[nodesCount][nodesCount];
      c = new short[nodesCount][nodesCount];

      this.initializeWeight(d, p, c);

      FloydWarshall.fw(nodesCount, d, p, c);

      FloydWarshall.storeMatrices(p, c, d, nodes.asMap());

   }

   /**
    * Initializes weights in transitive closure matrix.
    * 
    * @param d
    *           empty distances matrix.
    * @param p
    *           empty predecessor matrix.
    */
   private void initializeWeight(final float[][] d, final short[][] p,
         final short[][] c) {
      final int nodesCount = VersantGraph.this.nodes.size();
      for (int i = 0; i < nodesCount; i++) {
         Arrays.fill(d[i], Float.MAX_VALUE);
         Arrays.fill(c[i], (short) 0);
      }
      for (final VersantEdge currentEdge : VersantGraph.this.getEdges()) {
      	   int sID = currentEdge.getSource().getBasicId() - 1;
    	   int tID = currentEdge.getTarget().getBasicId() - 1;
         d[sID][tID] = currentEdge.getBasicValue();
         p[sID][tID] = VersantGraph.NEIGHBOUR_NODE;
         c[sID][tID] = 1;

      }
   }

//   public HashMap<Object, RowIndex> getRowIndices() {
//      // TODO Auto-generated method stub
//      return null;
//   }
//
//   public void insertRowIndex(final RowIndex ri) {
//      // TODO Auto-generated method stub
//
//   }

   /**
    * Returns a shortest path between 2 nodes using this graph's predecessor
    * matrix.
    * 
    * @param source
    *           source node.
    * @param target
    *           target node.
    * @return List of nodes in the path.
    */
   public ArrayList<Integer> shortestPath(final int source, final int target) {
	   zooActivateRead();

      ArrayList<Integer> path = new ArrayList<Integer>();

      final int predecessor = this.nodes.get(source).getRowIndex().get(target)
            .getPredecessor();

      if (predecessor == VersantGraph.UNREACHABLE_NODE) {
         return path;
      }

      if (predecessor != VersantGraph.NEIGHBOUR_NODE) {
         path = this.getIntermediatePathRowIndex(source, target);
      }

      path.add(0, source);
      path.add(target);

      return path;
   }

   /**
    * Returns incomplete shortest path between 2 nodes using this graph's
    * predecessor matrix.
    * 
    * @param source
    *           source node.
    * @param target
    *           target node.
    * @return list of nodes in the path.
    */
   private ArrayList<Integer> getIntermediatePathRowIndex(final Integer source,
         final Integer target) {
      final Map<Integer, EdgePropertiesImpl> ri = VersantGraph.this.getRowIndex(source);

      final ArrayList<Integer> path = new ArrayList<Integer>();
      //TODO 
      final Integer predecessor = ri.get(target).getPredecessor();
      if (ri.get(target).getPredecessor() > 0) {
         path.addAll(this.getIntermediatePathRowIndex(source, predecessor));
         path.add(predecessor);
         path.addAll(this.getIntermediatePathRowIndex(predecessor, target));
      }

      return path;
   }

}
