/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.SchemaClassProxy;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.query.QueryAdvice;
import org.zoodb.jdo.internal.query.QueryOptimizer;
import org.zoodb.jdo.internal.query.QueryParameter;
import org.zoodb.jdo.internal.query.QueryParser;
import org.zoodb.jdo.internal.query.QueryTerm;
import org.zoodb.jdo.internal.query.QueryTreeIterator;
import org.zoodb.jdo.internal.query.QueryTreeNode;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.ObjectIdentitySet;


/**
 * Query implementation.
 * 
 * @author Tilmann Zäschke
 */
public class QueryImpl implements Query {

	/** default. */
	private static final long serialVersionUID = 1L;

	// transient to satisfy findbugs (Query is Serializable, but _pm / _ext are not).
	private transient PersistenceManagerImpl pm;
	private transient Extent<?> ext;
	private boolean isUnmodifiable = false;
	private Class<?> candCls = ZooPCImpl.class; //TODO good default?
	private transient ZooClassDef candClsDef = null;
	private List<QueryAdvice> indexToUse = null;
	private String filter = "";
	
	private boolean unique = false;
	private boolean subClasses = true;
	private boolean ascending = true;
	private boolean ignoreCache = true;
	
	private final ObjectIdentitySet<Object> queryResults = new ObjectIdentitySet<Object>();

	private List<QueryParameter> parameters = new LinkedList<QueryParameter>();
	public QueryImpl(PersistenceManagerImpl pm, Extent ext, String filter) {
		this(pm);
		this.ext = ext;
		setClass( this.ext.getCandidateClass() );
		this.filter = filter;
	}

	public QueryImpl(PersistenceManagerImpl pm, Class cls,
			String arg1) {
		this(pm);
		setClass( cls );
		this.filter = arg1;
	}

	public QueryImpl(PersistenceManagerImpl pm) {
		this.pm = pm;
		ignoreCache = pm.getIgnoreCache();
	}

	/**
	 * SELECT [UNIQUE] [<result>] [INTO <result-class>]
        [FROM <candidate-class> [EXCLUDE SUBCLASSES]]
        [WHERE <filter>]
        [VARIABLES <variable declarations>]
        [PARAMETERS <parameter declarations>]
        [<import declarations>]
        [GROUP BY <grouping>]
        [ORDER BY <ordering>]
        [RANGE <start>, <end>]
	 * @param pm
	 * @param arg0
	 */
	public QueryImpl(PersistenceManagerImpl pm, String arg0) {
	    this.pm = pm;
	    
	    if (arg0==null || arg0 == "") {
	    	throw new NullPointerException("Please provide a query string.");
	    }
	    StringTokenizer st = new StringTokenizer(arg0);
	    
		String q = arg0.trim();
		String tok = st.nextToken();

		//SELECT
		if (!tok.toLowerCase().equals("select")) {
			throw new JDOUserException("Illegal token in query: \"" + tok + "\"");
		}
		q = q.substring(6).trim();
		tok = st.nextToken();

		//UNIQUE
		if (tok.toLowerCase().equals("unique")) {
			unique = true;
			q = q.substring(6).trim();
			tok = st.nextToken();
		}

		//INTO
		//TODO
		if (tok.toLowerCase().equals("into")) {
			//			_unique = true;
			//			q = q.substring(4).trim();
			//			tok = getToken(q);
			System.err.println("Query not supported: INTO");
		}

		//FROM
		if (tok.toLowerCase().equals("from")) {
			q = q.substring(4).trim();
			tok = st.nextToken();
			setClass( locateClass(tok) );
			q = q.substring(tok.length()).trim();
			if (!st.hasMoreTokens()) {
				return;
			}
			tok = st.nextToken();

			//EXCLUDE SUBCLASSES
			if (tok.toLowerCase().equals("exclude")) {
				q = q.substring(7).trim();
				tok = st.nextToken();
				if (!tok.toLowerCase().equals("subclasses")) {
					throw new JDOUserException("Illegal token in query, expected 'SUBCLASSES': \"" + 
							tok + "\"");
				}
				subClasses = false;
				q = q.substring(7).trim();
				tok = st.nextToken();
			}
		}

		//WHERE
		if (tok.toLowerCase().equals("where")) {
			q = q.substring(5).trim();
			this.filter = q;
			//TODO
		} else {
		    //maybe the query is finished?
		    if (!tok.toLowerCase().equals("")) {
		        throw new JDOUserException("Illegal token in query, expected 'WHERE': \"" + tok + 
		                "\"");
		    }
		}
	}

	private Class<?> locateClass(String className) {
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void addExtension(String key, Object value) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void addSubquery(Query sub, String variableDeclaration,
			String candidateCollectionExpression) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void addSubquery(Query sub, String variableDeclaration,
			String candidateCollectionExpression, String parameter) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void addSubquery(Query sub, String variableDeclaration,
			String candidateCollectionExpression, String... parameters) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void addSubquery(Query sub, String variableDeclaration,
			String candidateCollectionExpression, Map parameters) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void close(Object queryResult) {
		Object qr = queryResults.remove(queryResult);
		if (qr == null) {
			//TODO what does JDO say about this?
			DatabaseLogger.debugPrintln(0, "QueryResult not found.");
			return;
		}
		if (qr instanceof ExtentAdaptor) {
			((ExtentAdaptor<?>)qr).closeAll();
		} else if (qr instanceof ExtentImpl) {
			((ExtentImpl<?>)qr).closeAll();
		} else {
			//TODO ignore this
			DatabaseLogger.debugPrintln(0, "QueryResult not closable.");
		}
	}

	@Override
	public void closeAll() {
		while (!queryResults.isEmpty()) {
			close(queryResults.iterator().next());
		}
	}

	@Override
	public void compile() {
		checkUnmodifiable(); //? TODO ?

		//We do this on the query before assigning values to parameter.
		//Would it make sense to assign the values first and then properly parse the query???
		//Probably not: 
		//- every parameter change would require rebuilding the tree
		//- we would require an additional parser to assign the parameters
		QueryParser qp = new QueryParser(filter, candClsDef); 
		QueryTreeNode queryTree = qp.parseQuery();

		assignParametersToQueryTree(queryTree);
		//This is only for indices, not for given extents
		QueryOptimizer qo = new QueryOptimizer(candClsDef);
		indexToUse = qo.determineIndexToUse(queryTree);
	}

	@Override
	public void declareImports(String imports) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * For example:
	 * Query q = pm.newQuery (Employee.class, "salary > sal && name.startsWith(begin");
	 * q.declareParameters ("Float sal, String begin");
	 */
	@Override
	public void declareParameters(String parameters) {
		checkUnmodifiable();
		parameters = parameters.trim();
		int i1 = parameters.indexOf(',');
		//TODO check that paramerters do not alread exist. Overwrite them!
		while (i1 >= 0) {
			String p1 = parameters.substring(0, i1).trim();
			this.parameters .add(new QueryParameter(p1));
			parameters = parameters.substring(i1+1, parameters.length()).trim();
			i1 = parameters.indexOf(',');
		}
		this.parameters .add(new QueryParameter(parameters));
	}

	@Override
	public void declareVariables(String variables) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public long deletePersistentAll() {
		checkUnmodifiable(); //?
//		if (_ext != null && (_filter == null || _filter.isEmpty())) {
//			//deleting extent only
//			Session s = _pm.getSession();
//			int size = 0;
//			long oid;
//			//TODO
//			//improve: do not iterate OIDs
//			// instead: send class-delete command to Database
//			//         for cached objects, used list of cached objects in ZooClassDef to identify
//			//         them and mark them as deleted.
//			// But: How do we prevent objects from appearing in queries? A flag in ZooCLassDef
//			//  would only work if implement special treatment for objects that are created afterwards (??)
//			while ((oid=_ext.nextOid()) != Session.OID_NOT_ASSIGNED) {
//				size++;
//				//TODO
//				//delete oid
//			}
//			_pm.deletePersistentAll(c);
//			return size;
//		}
		Collection<?> c = (Collection<?>) execute();
		int size = 0;
		for (Object o: c) {
			size++;
		}
		pm.deletePersistentAll(c);
		return size;
	}

	@Override
	public long deletePersistentAll(Object... parameters) {
		checkUnmodifiable(); //?
		for (int i = 0; i < parameters.length; i++) {
			this.parameters.get(i).setValue(parameters[i]);
		}
		Collection<?> c = (Collection<?>) execute();
		int size = 0;
		for (Object o: c) {
			size++;
		}
		pm.deletePersistentAll(c);
		return size;
	}

	@Override
	public long deletePersistentAll(Map parameters) {
		checkUnmodifiable(); //?
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	private void assignParametersToQueryTree(QueryTreeNode queryTree) {
		QueryTreeIterator iter = queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			if (!term.isParametrized()) {
				continue;
			}
			String pName = term.getParamName();
			//TODO cache Fields in QueryTerm to avoid String comparison?
			boolean isAssigned = false;
			for (QueryParameter param: parameters) {
				if (pName.equals(param.getName())) {
					//TODO assigning a parameter instead of the value means that the query will
					//adapt new values even if it is not recompiled.
					term.setParameter(param);
					isAssigned = true;
					break;
				}
			}
			//TODO Exception?
			if (!isAssigned) {
				System.out.println("WARNING: Query parameter is not assigned: \"" + pName + "\"");
			}
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void applyQueryOnExtent(List<Object> ret, QueryAdvice qa) {
		QueryTreeNode queryTree = qa.getQuery();
		Iterator<?> ext2;
		if (qa.getIndex() != null) {
			//TODO other nodes...
			ext2 = pm.getSession().getPrimaryNode().readObjectFromIndex(qa.getIndex(),
					qa.getMin(), qa.getMax(), !ignoreCache);
			//System.out.println("Index: " + qa.getIndex().getName() + "  " + qa.getMin() + "/" + qa.getMax());
		} else {
			//use extent
			if (ext != null) {
				//use user-defined extent
				ext2 = ext.iterator();
			} else {
				//create type extent
				ext2 = new ExtentImpl(candCls, subClasses, pm, ignoreCache).iterator();
			}
		}
		
		if (ext != null && (!ext.hasSubclasses() || !ext.getCandidateClass().isAssignableFrom(candCls))) {
			final boolean hasSub = ext.hasSubclasses();
			final Class supCls = ext.getCandidateClass();
			// normal iteration
			while (ext2.hasNext()) {
				Object o = ext2.next();
				if (hasSub) {
					if (!supCls.isAssignableFrom(o.getClass())) {
						continue;
					}
				} else {
					if (supCls != o.getClass()) {
						continue;
					}
				}
				boolean isMatch = queryTree.evaluate(o);
				if (isMatch) {
					ret.add(o);
				}
			}
		} else {
			// normal iteration (ignoring the possibly existing compatible extent to allow indices)
			while (ext2.hasNext()) {
				Object o = ext2.next();
				boolean isMatch = queryTree.evaluate(o);
				if (isMatch) {
					ret.add(o);
				}
			}
		}
		if (ext2 instanceof CloseableIterator) {
			((CloseableIterator)ext2).close();
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object execute() {
		//no go through extent. Skip this if extent was generated on server from local filters.
		
		if (filter.equals("")) {
	        if (ext == null) {
	            ext = new ExtentImpl(candCls, subClasses, pm, ignoreCache);
	        }
			return new ExtentAdaptor(ext);
		}
		
		compile();

		//TODO can also return a list with (yet) unknown size. In that case size() should return
		//Integer.MAX_VALUE (JDO 2.2 14.6.1)
		ArrayList<Object> ret = new ArrayList<Object>();
		for (QueryAdvice qa: indexToUse) {
			applyQueryOnExtent(ret, qa);
		}
		
		//Now check if we need to check for duplicates, i.e. if multiple indices were used.
		for (QueryAdvice qa: indexToUse) {
			if (qa.getIndex() != indexToUse.get(0).getIndex()) {
				DatabaseLogger.debugPrintln(0, "Merging query results(A)!");
				System.out.println("Merging query results(A)!");
				Set<Object> ret2 = new ObjectIdentitySet<Object>();
				ret2.addAll(ret);
				return ret2;
			}
		}
		
		//If we have more than one sub-query, we need to merge anyway, because the result sets may
		//overlap. 
		//TODO implement merging of sub-queries!!!
		if (indexToUse.size() > 1) {
			DatabaseLogger.debugPrintln(0, "Merging query results(B)!");
			System.out.println("Merging query results(B)!");
			Set<Object> ret2 = new ObjectIdentitySet<Object>();
			ret2.addAll(ret);
			return ret2;
		}
		
		return ret;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1) {
		parameters.get(0).setValue(p1);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2) {
		parameters.get(0).setValue(p1);
		parameters.get(1).setValue(p2);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2, Object p3) {
		parameters.get(0).setValue(p1);
		parameters.get(1).setValue(p2);
		parameters.get(2).setValue(p3);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object executeWithArray(Object... parameters) {
		for (int i = 0; i < parameters.length; i++) {
			this.parameters.get(i).setValue(parameters[i]);
		}
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object executeWithMap(Map parameters) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getIgnoreCache() {
		return ignoreCache;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return pm;
	}

	@Override
	public boolean isUnmodifiable() {
		return isUnmodifiable;
	}

	@Override
	public void setCandidates(Extent pcs) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCandidates(Collection pcs) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setClass(Class cls) {
		checkUnmodifiable();
    	if (!ZooPCImpl.class.isAssignableFrom(cls)) {
    		throw new JDOUserException("Class is not persistence capabale: " + cls.getName());
    	}
		candCls = cls;
		Node node = pm.getSession().getPrimaryNode();
		SchemaClassProxy sch = pm.getSession().getSchemaManager().locateSchema(cls, node);
		if (sch == null) {
		    throw new JDOUserException("Class schema is not defined: " + cls.getName());
		}
		candClsDef = sch.getSchemaDef();
	}

	@Override
	public void setExtensions(Map extensions) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFilter(String filter) {
		checkUnmodifiable();
		this.filter = filter;
	}

	@Override
	public void setGrouping(String group) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setIgnoreCache(boolean ignoreCache) {
		this.ignoreCache = ignoreCache;;
	}

	@Override
	public void setOrdering(String ordering) {
		checkUnmodifiable();
		ascending = true;
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRange(String fromInclToExcl) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRange(long fromIncl, long toExcl) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setResult(String data) {
		checkUnmodifiable();
		QueryResultProcessor result = new QueryResultProcessor(data, candCls);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setResultClass(Class cls) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setUnique(boolean unique) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setUnmodifiable() {
		isUnmodifiable = true;
	}

	private void checkUnmodifiable() {
		if (isUnmodifiable) {
			throw new JDOUserException("This query is unmodifiable.");
		}
	}

}
