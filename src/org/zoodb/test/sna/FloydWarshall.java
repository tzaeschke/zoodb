package org.zoodb.test.sna;

import java.util.HashMap;
import java.util.LinkedHashMap;

public final class FloydWarshall {

	private FloydWarshall() {
	}

	public static void fw(final int nodesCount, final float[][] d,
			final short[][] p, final short[][] c) {
		for (int k = 0; k < nodesCount; k++) {
			for (int i = 0; i < nodesCount; i++) {
				final float dik = d[i][k];
				if (dik != Float.MAX_VALUE) {
					for (int j = 0; j < nodesCount; j++) {
						if (d[k][j] != Float.MAX_VALUE && dik + d[k][j] <= d[i][j]) {
							if (dik + d[k][j] == d[i][j]) {
								c[i][j]++;
							} else {
								c[i][j] = 1;
								d[i][j] = dik + d[k][j];
								p[i][j] = (short) (k + 1);
							}
						}
					}
				}
			}
		}
	}

	public static double betweennessCentralityDirected(int nodeId,
			HashMap<Object, VersantNode> nodes) {
		double sum = 0.0;

		for (int i = 1; i < nodes.size() + 1; i++) {
			for (int j = 1; j < nodes.size() + 1; j++) {

				if (i != j && i != nodeId && j != nodeId) {
					final VersantNode n1 = nodes.get(i);

					final EdgePropertiesImpl ep1 = n1.getRowIndex().get(j);
					final EdgePropertiesImpl ep2 = n1.getRowIndex().get(nodeId);

					if (ep1.getDistance() > ep2.getDistance()) {
						final VersantNode n3 = nodes.get(nodeId);
						final EdgePropertiesImpl ep3 = n3.getRowIndex().get(j);

						if (ep2.getDistance() + ep3.getDistance() == ep1
								.getDistance()) {
							sum += (double) (ep2.getPathCount() * ep3.getPathCount())
							/ ep1.getPathCount();
						}
					}
				}
			}
		}
		return sum;
	}

	public static double betweennessCentralityUndirected(int nodeId,
			HashMap<Object, VersantNode> nodes) {
		double sum = 0.0;

		int nSize = nodes.size();
		final VersantNode n3 = nodes.get(nodeId);
		final HashMap<Integer, EdgePropertiesImpl> rowIndexN3 = n3.getRowIndex();
		for (int i = 1; i < nSize + 1; i++) {
			if (i != nodeId) {
				final HashMap<Integer, EdgePropertiesImpl> rowIndexN1 = 
				    nodes.get(i).getRowIndex();
				final EdgePropertiesImpl ep2 = rowIndexN1.get(nodeId);
				for (int j = i + 1; j < nSize + 1; j++) {
					if (j != nodeId) {
						final EdgePropertiesImpl ep1 = rowIndexN1.get(j);
						if (ep1.getDistance() > ep2.getDistance()) {
							final EdgePropertiesImpl ep3 = rowIndexN3.get(j);
							if (ep2.getDistance() + ep3.getDistance() == ep1.getDistance()) {
								sum += (double) (ep2.getPathCount() * ep3.getPathCount())
								/ ep1.getPathCount();
							}
						}
					}
				}
			}
		}
		return 2 * sum;
	}

	public static void storeMatrices(final short[][] p, final short[][] c,
			final float[][] d, HashMap<Object, VersantNode> nodes) {
		for (int i = 0; i < d.length; i++) {
			final VersantNode current = nodes.get(i + 1);

			if (i % 10 == 0) {
				DBPopulate.commit();
				if (i % 100 == 0) {
					DBPopulate.cleanCache();
				}
			}

			for (int j = 0; j < d.length; j++) {
				// System.out.print(p[i][j] + " ");

				if (i == j) {
					p[i][j] = VersantGraph.DISTANCETOSELF;
					d[i][j] = VersantGraph.DISTANCETOSELF;
					c[i][j] = VersantGraph.DISTANCETOSELF;
				}

				final EdgePropertiesImpl prop = new EdgePropertiesImpl();
				prop.setPathCount(c[i][j]);

				if (d[i][j] != Float.MAX_VALUE) {
					prop.setDistance(d[i][j]);
				} else {
					prop.setDistance(VersantGraph.UNREACHABLE_NODE);
				}
				if (p[i][j] > 0) {
					prop.setPredecessor(p[i][j]);
				} else if (p[i][j] == VersantGraph.NEIGHBOUR_NODE) {
					prop.setPredecessor(p[i][j]);
					current.setNeighbourCount();
				} else if (p[i][j] == VersantGraph.UNREACHABLE_NODE) {

					prop.setPredecessor(VersantGraph.UNREACHABLE_NODE);
				}
				current.addEdgeProperty(j + 1, prop);

			}

		}

	}

}
