package org.zoodb.profiling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassMergeSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.ClassSplitSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.CollectionAggregationSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.DuplicateSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.FieldRemovalSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.LOBSuggestion;
import ch.ethz.globis.profiling.commons.suggestion.RefPathItem;
import ch.ethz.globis.profiling.commons.suggestion.ReferenceShortcutSuggestion;

public class ProfUtil {

	private static class Pair implements Comparable<Pair> {
		private final double r;
		private final String msg;
		
		Pair(double r, String msg) {
			this.r = r;
			this.msg = msg;
		}
		
		@Override
		public boolean equals(Object obj) {
			return r == ((Pair)obj).r;
		}
		
		@Override
		public int compareTo(Pair o) {
			return (int) (r - o.r);
		}		
	}
	
	public static void listSuggestions(Collection<AbstractSuggestion> suggestions) {
		int nS = 0;
		ArrayList<Pair> map = new ArrayList<Pair>();
		for (AbstractSuggestion s: suggestions) {
			double r = Math.round(s.getGain()/s.getCost()*10.)/10.;
			if (Double.isNaN(r)) {
				r = -1;
			}
			String cg = "  c/g=" + s.getGain() + "/" + s.getCost() + " = "  + r;
			String cn = cutPackage(s.getClazzName());
			String msg;
			//System.out.println("name=" + s.getClass().getName() + "  --> " + s.getClazzName());
			if (s instanceof FieldRemovalSuggestion) {
				FieldRemovalSuggestion frs = (FieldRemovalSuggestion) s;
				msg = "RemovalF: " + cn + "." + frs.getFieldName(); 
			} else if (s instanceof ClassRemovalSuggestion) {
				//ClassRemovalSuggestion frs = (ClassRemovalSuggestion) s;
				msg = "RemovalC: " + cn; 
			} else if (s instanceof ReferenceShortcutSuggestion) {
				ReferenceShortcutSuggestion frs = (ReferenceShortcutSuggestion) s;
				msg = "Shortcut: " + cn + " to " + cutPackage(frs.getRefTarget()) + " [";
				for (RefPathItem i: frs.getItems()) {
					msg += i.getFieldName() + "->" + cutPackage(i.getClassName()) + "(" +
							i.getTraversalCount() + "),";
				}
				msg += "]";
			} else if (s instanceof CollectionAggregationSuggestion) {
				CollectionAggregationSuggestion frs = (CollectionAggregationSuggestion) s;
				msg = "Aggregation: " + cn + "." + frs.getParentField() + " < " + 
				cutPackage(frs.getAggregateeClass()) + "." + frs.getAggregateeField();
			} else if (s instanceof DuplicateSuggestion) {
				DuplicateSuggestion frs = (DuplicateSuggestion) s;
				msg = "Duplicate: " + cn + "." + frs.getParentField() + " < " + 
				cutPackage(frs.getDuplicateeClass()) + "." + frs.getDuplicateeField();
			} else if (s instanceof LOBSuggestion) {
				LOBSuggestion frs = (LOBSuggestion) s;
				msg = "LOB: " + cn + "." + frs.getFieldName() + ":" + 
				frs.getDetectionCount() + "/" + frs.getAvgLobSize();
			} else if (s instanceof ClassMergeSuggestion) {
				ClassMergeSuggestion frs = (ClassMergeSuggestion) s;
				msg = "ClassMerge: " + cn + " : " + cutPackage(frs.getMasterClass()) + "+" + 
						cutPackage(frs.getMergeeClass());
			} else if (s instanceof ClassSplitSuggestion) {
				ClassSplitSuggestion frs = (ClassSplitSuggestion) s;
				msg = "ClassSplit: " + cn + " : [";
				for (String f: frs.getMasterFields()) {
					msg += f + ", ";
				}
				msg += "] / [";
				for (String f: frs.getOutsourcedFields()) {
					msg += f + ", ";
				}
				msg += "]";
			} else {
				throw new IllegalArgumentException("Unknown: " + s.getClass().getName());
			}
			nS++;
			msg += cg;
			map.add(new Pair(r, msg));
		}
		Collections.sort(map);
		for (Pair p: map) {
			System.out.println(p.msg);
		}
		System.out.println("Suggestions: " + nS);
	}

	private static String cutPackage(String className) {
		return className.substring(className.lastIndexOf('.')+1);
	}

	
}
