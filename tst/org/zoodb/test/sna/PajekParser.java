/*
 * @(#)PajekParser.java   1.0   Dec 8, 2010
 *
 * Copyright 2010-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) $Id: PajekParser.java 1995 2011-10-12 20:09:21Z D\michagro $
 */
package org.zoodb.test.sna;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Parses a file in the Pajek format and uses a GraphBuilder to generate output.
 * 
 * @author Michael Grossniklaus &lt;grossniklaus@inf.ethz.ch&gt;
 * @version 1.0
 */
public class PajekParser {

   private static final char COMMENT_MARKER = '%';
   private static final char CONTEXT_MARKER = '*';

   private final VersantBuilder builder;

   public PajekParser(final VersantBuilder builder) {
      this.builder = builder;
   }

   public void parse(final File file) throws IOException {
      this.parse(new FileInputStream(file));
   }

   public void parse(final String name) throws IOException {
      final File file = new File(name);
      if (file.exists()) {
         this.parse(file);
      } else {
//         final ClassLoader loader = ClassLoader.getSystemClassLoader();
//         final InputStream in = loader.getResourceAsStream(name);
         final InputStream in = this.getClass().getResourceAsStream(name);
         this.parse(in);
      }
   }

   private void parse(final InputStream in) throws IOException {
      final ParseState state = new ParseState();
      String line = null;
      final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      while ((line = reader.readLine()) != null) {
         line = line.trim();
         if (line.length() > 0) {
            final char marker = line.charAt(0);
            switch (marker) {
               case COMMENT_MARKER:
                  break;
               case CONTEXT_MARKER:
                  if (line.length() > 1) {
                     final String[] tokens = line.split("[ ]+");
                     final ParseContext context = ParseContext.valueOf(tokens[0]
                           .substring(1).toUpperCase());
                     switch (context) {
                        case VERTICES:
                           if (tokens.length > 1) {
                              state.setTotalNodes(Long.valueOf(tokens[1]));
                              if (tokens.length > 2) {
                                 state.setPartitionIndex(Long.valueOf(tokens[2]));
                              } else {
                                 state.setPartitionIndex(state.getTotalNodes());
                              }
                           }
                           state.resetCurrentNode();
                           break;
                        case ARCS: // Do nothing
                        case EDGES: // Do nothing
                        case ARCSLIST: // Do nothing
                        case EDGESLIST: // Do nothing
                        case MATRIX:
                           state.resetCurrentNode();
                        default: // This case *should* not occur
                     }
                     state.setContext(context);
                  }
                  break;
               default:
                  this.parseLine(state, line);
            }
         }
      }
      reader.close();
   }

   private void parseLine(final ParseState state, final String line) {
      final String content = line;
      Object[] tokens;
      switch (state.getContext()) {
         case VERTICES:
            tokens = this.parseVertex(content);
            if (tokens != null && tokens.length > 1) {
               this.builder.handleNode((Long) tokens[0], (String) tokens[1]);
            }
            state.incrementCurrentNode();
            break;
         case ARCS:
            tokens = this.parseEdge(content);
            if (tokens != null && tokens.length > 2) {
               this.builder
                     .handleArc((Long) tokens[0], (Long) tokens[1], (Double) tokens[2]);
            }
            break;
         case EDGES:
            tokens = this.parseEdge(content);
            if (tokens != null && tokens.length > 2) {
               this.builder.handleEdge((Long) tokens[0], (Long) tokens[1],
                     (Double) tokens[2]);
            }
            break;
         case ARCSLIST:
            tokens = this.parseEdgesList(content);
            if (tokens != null && tokens.length > 1) {
               final Long source = (Long) tokens[0];
               for (int i = 1; i < tokens.length; i++) {
                  this.builder.handleArc(source, (Long) tokens[i], 1.0);
               }
            }
            break;
         case EDGESLIST:
            tokens = this.parseEdgesList(content);
            if (tokens != null && tokens.length > 1) {
               final Long source = (Long) tokens[0];
               for (int i = 1; i < tokens.length; i++) {
                  this.builder.handleEdge(source, (Long) tokens[i], 1.0);
               }
            }
            break;
         case MATRIX:
            tokens = this.parseMatrixRow(content);
            if (tokens != null && tokens.length == state.getPartitionSize()) {
               final long start = state.getPartitionStartNode();
               final Long source = Long.valueOf(state.getCurrentNode());
               for (int i = 0; i < tokens.length; i++) {
                  final Long target = Long.valueOf(i + start);
                  this.builder.handleEdge(source, target, (Double) tokens[i]);
               }
               state.incrementCurrentNode();
            }
            break;
         default:
            // This case *should* not occur
      }
   }

   Object[] parseVertex(final String line) {
      final int splitPos = line.indexOf(" ");
      if (splitPos > 0) {
         final String id = line.substring(0, splitPos);
         String label = line.substring(splitPos + 1);
         int endPos;
         if (label.startsWith("\"")) {
            endPos = label.indexOf("\"", 1);
         } else {
            endPos = label.indexOf(" ", 0);
         }
         label = label.substring(1, endPos);
         return new Object[] { Long.valueOf(id), label };
      }
      return null;
   }

   Object[] parseEdge(final String line) {
      final String[] tokens = line.split("[ ]+");
      if (tokens.length > 1) {
         Double value = null;
         if (tokens.length > 2) {
            value = Double.valueOf(tokens[2]);
         }
         return new Object[] { Long.valueOf(tokens[0]), Long.valueOf(tokens[1]), value };
      }
      return null;
   }

   Object[] parseEdgesList(final String line) {
      final String[] tokens = line.split("[ ]+");
      if (tokens.length > 1) {
         final Object[] result = new Object[tokens.length];
         for (int i = 0; i < tokens.length; i++) {
            result[i] = Long.valueOf(tokens[i]);
         }
         return result;
      }
      return null;
   }

   Object[] parseMatrixRow(final String line) {
      final String[] tokens = line.split("[ ]+");
      final Object[] result = new Object[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
         result[i] = Double.valueOf(tokens[i]);
      }
      return result;
   }

   private enum ParseContext {
      VERTICES, ARCS, ARCSLIST, EDGES, EDGESLIST, MATRIX
   };

   private class NodePartition {

      private final long startNode;
      private final long endNode;

      NodePartition(final long startNode, final long endNode) {
         this.startNode = startNode;
         this.endNode = endNode;
      }

      long getStartNode() {
         return this.startNode;
      }

//      long getEndNode() {
//         return this.endNode;
//      }

      long getSize() {
         return this.endNode - this.startNode + 1;
      }
   }

   private class ParseState {

      private ParseContext context;
      private long totalNodes;
      private long currentNode;
      private final NodePartition[] partitions;

      ParseState() {
         this.context = null;
         this.totalNodes = 0;
         this.currentNode = 0;
         // At the moment, I am only aware of graphs with two partitions.
         this.partitions = new NodePartition[2];
      }

      ParseContext getContext() {
         return this.context;
      }

      void setContext(final ParseContext context) {
         this.context = context;
      }

      long getTotalNodes() {
         return this.totalNodes;
      }

      void setTotalNodes(final long totalNodes) {
         this.totalNodes = totalNodes;
      }

      long getCurrentNode() {
         return this.currentNode;
      }

      void resetCurrentNode() {
         this.currentNode = 1;
      }

      void incrementCurrentNode() {
         this.currentNode++;
      }

      void setPartitionIndex(final long index) {
         this.partitions[0] = new NodePartition(1, index);
         if (index < this.totalNodes) {
            this.partitions[1] = new NodePartition(index + 1, this.totalNodes);
         }
      }

      long getPartitionSize() {
         if (this.isPartitioned()) {
            return this.partitions[1].getSize();
         }
         return this.partitions[0].getSize();
      }

      long getPartitionStartNode() {
         if (this.isPartitioned()) {
            return this.partitions[1].getStartNode();
         }
         return this.partitions[0].getStartNode();
      }

      private boolean isPartitioned() {
         return this.partitions[1] != null && this.partitions[1].getSize() > 0;
      }
   }
}
