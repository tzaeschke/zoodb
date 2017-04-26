/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.query.QueryAdvice;
import org.zoodb.internal.query.QueryComparator;
import org.zoodb.internal.query.QueryMergingIterator;
import org.zoodb.internal.query.QueryOptimizer;
import org.zoodb.internal.query.QueryParameter;
import org.zoodb.internal.query.QueryParameter.DECLARATION;
import org.zoodb.internal.query.QueryParser;
import org.zoodb.internal.query.QueryParserV3;
import org.zoodb.internal.query.QueryTerm;
import org.zoodb.internal.query.QueryTreeIterator;
import org.zoodb.internal.query.QueryTreeNode;
import org.zoodb.internal.query.TypeConverterTools;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.ObjectIdentitySet;
import org.zoodb.internal.util.Pair;
import org.zoodb.internal.util.SynchronizedROCollection;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;


/**
 * Query implementation.
 * 
 * @author Tilmann Zaeschke
 */
public class QueryImpl implements Query {

	/** default. */
	private static final long serialVersionUID = 1L;

	// transient to satisfy findbugs (Query is Serializable, but _pm / _ext are not).
	private transient PersistenceManagerImpl pm;
	private transient Extent<?> ext;
	private boolean isUnmodifiable = false;
	private Class<?> candCls = ZooPC.class; //TODO good default?
	private transient ZooClassDef candClsDef = null;
	private List<QueryAdvice> indexToUse = null;
	private String filter = "";
	
	private boolean unique = false;
	private boolean subClasses = true;
	private boolean ignoreCache = true;
	private List<Pair<ZooFieldDef, Boolean>> ordering = new ArrayList<>();
	private String orderingStr = null;
	
	private String resultSettings = null;
	private Class<?> resultClass = null;
	
	private final ObjectIdentitySet<Object> queryResults = new ObjectIdentitySet<Object>();

	private List<QueryParameter> parameters = new ArrayList<QueryParameter>();
	
	private QueryTreeNode queryTree;
	//This is used in schema auto-create mode when the persistent class has no schema defined
	private boolean isDummyQuery = false;
	
	private long rangeMin = 0;
	private long rangeMax = Long.MAX_VALUE;
	private QueryParameter rangeMinParameter = null;
	private QueryParameter rangeMaxParameter = null;
	private String rangeStr = null;
	
	@SuppressWarnings("rawtypes")
	public QueryImpl(PersistenceManagerImpl pm, Extent ext, String filter) {
		this(pm);
		this.ext = ext;
		setClass( this.ext.getCandidateClass() );
		this.filter = filter;
		this.subClasses = ext.hasSubclasses();
	}

	@SuppressWarnings("rawtypes")
	public QueryImpl(PersistenceManagerImpl pm, Class cls,
			String arg1) {
		this(pm);
		setClass( cls );
		this.filter = arg1;
	}

	public QueryImpl(PersistenceManagerImpl pm) {
		this.pm = pm;
		ignoreCache = pm.getIgnoreCache();
		pm.getSession().checkActiveRead();
	}

	/**
	 * {@code
	 * SELECT [UNIQUE] [<result>] [INTO <result-class>]
        [FROM <candidate-class> [EXCLUDE SUBCLASSES]]
        [WHERE <filter>]
        [VARIABLES <variable declarations>]
        [PARAMETERS <parameter declarations>]
        [<import declarations>]
        [GROUP BY <grouping>]
        [ORDER BY <ordering>]
        [RANGE <start>, <end>]
        }
	 * @param pm
	 * @param arg0
	 */
	public QueryImpl(PersistenceManagerImpl pm, String arg0) {
	    this(pm);
	    
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
			throw new JDOUserException("Class not found: " + className, e);
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

	@SuppressWarnings("rawtypes")
	@Override
	public void addSubquery(Query sub, String variableDeclaration,
			String candidateCollectionExpression, Map parameters) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void close(Object queryResult) {
		if (!queryResults.remove(queryResult)) {
			//TODO what does JDO say about this?
			DBLogger.debugPrintln(0, "QueryResult not found.");
			return;
		}
		if (queryResult instanceof ExtentAdaptor) {
			((ExtentAdaptor<?>)queryResult).closeAll();
		} else if (queryResult instanceof ExtentImpl) {
			((ExtentImpl<?>)queryResult).closeAll();
		} else {
			//TODO ignore this
			DBLogger.debugPrintln(0, "QueryResult not closable.");
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
		checkUnmodifiable();
		resetQuery();
		compileQuery();
	}
	
	private void compileQuery() {
		//compile only if it was not already compiled, unless the filter changed...
		if (queryTree != null) {
			return;
		}
		
		String fStr = filter;
		if (rangeStr != null) {
			fStr = (fStr == null) ? "" : fStr; 
			fStr += " range " + rangeStr; 
		}
		if (orderingStr != null) {
			fStr = (fStr == null) ? "" : fStr; 
			fStr += " order by " + orderingStr; 
		}

		if (fStr == null || fStr.trim().length() == 0 || isDummyQuery) {
			return;
		}
		
		if (DBStatistics.isEnabled()) {
			pm.getSession().statsInc(DBStatistics.STATS.QU_COMPILED);
		}
		
		//We do this on the query before assigning values to parameter.
		//Would it make sense to assign the values first and then properly parse the query???
		//Probably not: 
		//- every parameter change would require rebuilding the tree
		//- we would require an additional parser to assign the parameters
		//QueryParser qp = new QueryParser(filter, candClsDef, parameters, ordering);
		//QueryParserV2 qp = new QueryParserV2(filter, candClsDef, parameters, ordering); 
		QueryParserV3 qp = 
				new QueryParserV3(fStr, candClsDef, parameters, ordering, rangeMin, rangeMax);
		queryTree = qp.parseQuery();
		rangeMin = qp.getRangeMin();
		rangeMax = qp.getRangeMax();
		rangeMinParameter = qp.getRangeMinParam();
		rangeMaxParameter = qp.getRangeMaxParam();
	}
	
	private void resetQuery() {
		//See Test_122: We need to clear this for setFilter() calls
		for (int i = 0; i < parameters.size(); i++) {
			QueryParameter p = parameters.get(i);
			if (p.getDeclaration() != DECLARATION.API) {
				parameters.remove(i);
				i--;
			}
		}
		rangeMinParameter = null;
		rangeMaxParameter = null;
		queryTree = null;
		ordering.clear();
	}

	@Override
	public void declareImports(String imports) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * For example:
	 * Query q = pm.newQuery (Employee.class, "salary = sal && name.startsWith(begin");
	 * q.declareParameters ("Float sal, String begin");
	 */
	@Override
	public void declareParameters(String paramString) {
		checkUnmodifiable();
		parameters.clear();
		paramString = paramString.trim();
		int i1 = paramString.indexOf(',');
		while (i1 >= 0) {
			String p1 = paramString.substring(0, i1).trim();
			updateParameterDeclaration(p1);
			paramString = paramString.substring(i1+1, paramString.length()).trim();
			i1 = paramString.indexOf(',');
		}
		updateParameterDeclaration(paramString);
		resetQuery();
	}

	private void updateParameterDeclaration(String paramDecl) {
		int i = paramDecl.indexOf(' ');
		String type = paramDecl.substring(0, i);
		String name = paramDecl.substring(i+1);
		if (name.startsWith(":")) {
			throw new JDOUserException("Illegal parameter name: " + name);
		}
		for (QueryParameter p: parameters) {
			if (p.getName().equals(name)) {
				throw new JDOUserException("Duplicate parameter name: " + name);
			}
		}
		Class<?> cls = QueryParser.locateClassFromShortName(type);
		parameters.add(new QueryParameter(cls, name, DECLARATION.API));
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
			pm.deletePersistent(o);
		}
		return size;
	}

	@Override
	public long deletePersistentAll(Object... parameters) {
		checkUnmodifiable(); //?
		Collection<?> c = (Collection<?>) executeWithArray(parameters);
		pm.deletePersistentAll(c);
		return c.size();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public long deletePersistentAll(Map parameters) {
		checkUnmodifiable(); //?
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	private void assignParametersToQueryTree(QueryTreeNode queryTree) {
		if (parameters.isEmpty()) {
			return;
		}
		//TODO
		//TODO
		//TODO
		//TODO We should install an subscription service here. Every term/function that
		//TODO uses a QueryParameter should subscribe to the Parameter and get updated when
		//TODO the parameter changes. Parameters withou subscriptions should cause errors/warnings.
		//TODO
		//TODO
		//TODO
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
					//check
					if (param.getType() == null) {
						throw new JDOUserException(
								"Parameter has not been declared: " + param.getName());
					}
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
		if (!ignoreCache) {
			ClientSessionCache cache = pm.getSession().internalGetCache();
			cache.persistReachableObjects();
		}
		if (qa.getIndex() != null) {
			ext2 = pm.getSession().getPrimaryNode().readObjectFromIndex(qa.getIndex(),
					qa.getMin(), qa.getMax(), !ignoreCache);
			if (!ignoreCache) {
				ClientSessionCache cache = pm.getSession().internalGetCache();
				ArrayList<ZooPC> dirtyObjs = cache.getDirtyObjects();
				if (!dirtyObjs.isEmpty()) {
					QueryMergingIterator<ZooPC> qmi = new QueryMergingIterator();
					qmi.add((Iterator<ZooPC>) ext2);
					qmi.add(cache.iterator(candClsDef, subClasses, ObjectState.PERSISTENT_NEW));
					ext2 = qmi;
				}
			}
		} else {
			if (DBLogger.isLoggable(Level.FINE)) {
				DBLogger.LOGGER.fine("query.execute() uses extent without index");
			}
			if (DBStatistics.isEnabled()) {
				pm.getSession().statsInc(STATS.QU_EXECUTED_WITHOUT_INDEX);
				if (!ordering.isEmpty()) {
					pm.getSession().statsInc(STATS.QU_EXECUTED_WITH_ORDERING_WITHOUT_INDEX);
				}
			}
			//use extent
			if (ext != null) {
				//use user-defined extent
				ext2 = ext.iterator();
			} else {
				//create type extent
				ext2 = new ExtentImpl(candCls, subClasses, pm, ignoreCache).iterator();
			}
		}
		
		if (ext != null && !subClasses) {
			// normal iteration
			while (ext2.hasNext()) {
				Object o = ext2.next();
				if (subClasses) {
					if (!candCls.isAssignableFrom(o.getClass())) {
						continue;
					}
				} else {
					if (candCls != o.getClass()) {
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
	
	private void checkParamCount(int i) {
		//this needs to be checked AFTER query compilation
		int max = parameters.size();
		if (i > max) {
			throw new JDOUserException("Too many arguments given, parameter count: " + max);
		}
		if (i < max) {
			throw new JDOUserException("Too few arguments given, parameter count: " + max + 
					". In case of a String query, consider putting the argument in \" or '." +
					"Params: " + Arrays.toString(parameters.toArray()));
		}
	}
	
	private Object runQuery() {
		if (DBStatistics.isEnabled()) {
			pm.getSession().statsInc(STATS.QU_EXECUTED_TOTAL);
		}
		long t1 = System.nanoTime();
		try {
			pm.getSession().lock();
			pm.getSession().checkActiveRead();
			if (isDummyQuery) {
				//empty result if no schema is defined (auto-create schema)
				//TODO check cached objects
				return new LinkedList<Object>();
			}

			//assign parameters
			assignParametersToQueryTree(queryTree);
			//This is only for indices, not for given extents
			QueryOptimizer qo = new QueryOptimizer(candClsDef);
			indexToUse = qo.determineIndexToUse(queryTree);

			//TODO can also return a list with (yet) unknown size. In that case size() should return
			//Integer.MAX_VALUE (JDO 2.2 14.6.1)
			ArrayList<Object> ret = new ArrayList<Object>();
			for (QueryAdvice qa: indexToUse) {
				applyQueryOnExtent(ret, qa);
			}

			//Now check if we need to check for duplicates, i.e. if multiple indices were used.
			for (QueryAdvice qa: indexToUse) {
				if (qa.getIndex() != indexToUse.get(0).getIndex()) {
					DBLogger.debugPrintln(0, "Merging query results(A)!");
					System.out.println("Merging query results(A)!");
					ObjectIdentitySet<Object> ret2 = new ObjectIdentitySet<Object>();
					ret2.addAll(ret);
					return postProcess(ret2);
				}
			}

			//If we have more than one sub-query, we need to merge anyway, because the result sets may
			//overlap. 
			//TODO implement merging of sub-queries!!!
			if (indexToUse.size() > 1) {
				DBLogger.debugPrintln(0, "Merging query results(B)!");
				System.out.println("Merging query results(B)!");
				ObjectIdentitySet<Object> ret2 = new ObjectIdentitySet<Object>();
				ret2.addAll(ret);
				return postProcess(ret2);
			}

			return postProcess(ret);
		} finally {
			pm.getSession().unlock();
			if (DBLogger.isLoggable(Level.FINE)) {
				long t2 = System.nanoTime();
				DBLogger.LOGGER.fine("query.execute(): Time=" + (t2-t1) + 
						"ns; Class=" + candCls + "; filter=" + filter);
			}
		}
	}

	private Object postProcess(Collection<Object> c) {
		if (resultSettings != null) {
			QueryResultProcessor rp = 
					new QueryResultProcessor(resultSettings, candCls, candClsDef, resultClass);
			if (rp.isProjection()) {
				c = rp.processResultProjection(c.iterator(), unique);
			} else {
				//must be an aggregate
				Object o = rp.processResultAggregation(c.iterator());
				return o;
			}
		}
		if (unique) {
			//unique
			Iterator<Object> iter = c.iterator();
			if (iter.hasNext()) {
				Object ret = iter.next();
				if (iter.hasNext()) {
					throw new JDOUserException("Too many results found in unique query.");
				}
				return ret;
			} else {
				//no result found
				return null;
			}
		}
		if (ordering != null && !ordering.isEmpty()) {
			if (!(c instanceof List)) {
				c = new ArrayList<>(c);
			}
			Collections.sort((List<Object>) c, new QueryComparator<Object>(ordering));
		}
		
		//get range
		if (rangeMinParameter != null) {
			rangeMin = TypeConverterTools.toLong(rangeMinParameter.getValue());
		}
		if (rangeMaxParameter != null) {
			rangeMax = TypeConverterTools.toLong(rangeMaxParameter.getValue());
		}
		
		//To void remove() calls
		return new SynchronizedROCollection<>(c, pm.getSession(), rangeMin, rangeMax);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object execute() {
		//now go through extent. Skip this if extent was generated on server from local filters.
		filter = filter.trim();
		if (filter.length() == 0 && orderingStr == null && !isDummyQuery) {
			try {
				pm.getSession().lock();
				pm.getSession().checkActiveRead();
				if (!ignoreCache) {
					ClientSessionCache cache = pm.getSession().internalGetCache();
					cache.persistReachableObjects();
				}
				if (ext == null) {
					ext = new ExtentImpl(candCls, subClasses, pm, ignoreCache);
				}
				if (DBStatistics.isEnabled()) {
					pm.getSession().statsInc(STATS.QU_EXECUTED_TOTAL);
					pm.getSession().statsInc(STATS.QU_EXECUTED_WITHOUT_INDEX);
				}
				return postProcess(new ExtentAdaptor(ext));
			} finally {
				pm.getSession().unlock();
			}
		}
		
		compileQuery();
		checkParamCount(0);
		return runQuery();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1) {
		return executeWithArray(p1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2) {
		return executeWithArray(p1, p2);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2, Object p3) {
		return executeWithArray(p1, p2, p3);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object executeWithArray(Object... parameters) {
		compileQuery();
		checkParamCount(parameters.length);
		for (int i = 0; i < parameters.length; i++) {
			this.parameters.get(i).setValue(parameters[i]);
		}
		return runQuery();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Object executeWithMap(Map parameters) {
		compileQuery();
		checkParamCount(parameters.size());
		for (QueryParameter p: this.parameters) {
			p.setValue(parameters.get(p.getName()));
		}
		return runQuery();
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

	@SuppressWarnings("rawtypes")
	@Override
	public void setCandidates(Extent pcs) {
		checkUnmodifiable();
		this.ext = pcs;
		if (pcs.getCandidateClass() != candCls) {
			setClass( pcs.getCandidateClass() );
			resetQuery();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setCandidates(Collection pcs) {
		checkUnmodifiable();
		ext = new CollectionExtent(pcs, pm, ignoreCache);
		if (ext.getCandidateClass() != candCls) {
			setClass(ext.getCandidateClass());
			resetQuery();
		}
//		
//		if (pcs.isEmpty()) {
//			ext = pcs;
//			setClass(ZooPC.class);
//		}
//		Iterator<?> iter = pcs.iterator();
//		Object o1 = iter.next();
//		Class<?> cls = o1.getClass();
//    	if (!ZooPC.class.isAssignableFrom(cls)) {
//    		throw DBLogger.newUser("Class is not persistence capabale: " + cls.getName());
//    	}
//    	if (pm != JDOHelper.getPersistenceManager(o1)) {
//    		throw DBLogger.newUser("The object belongs to another PersistenceManager");
//    	}
//    	while (iter.hasNext()) {
//    		Object o2 = iter.next();
//    		Class<?> cls2 = o2.getClass();
//        	if (!ZooPC.class.isAssignableFrom(cls2)) {
//        		throw DBLogger.newUser("Class is not persistence capabale: " + cls.getName());
//        	}
//        	if (pm != JDOHelper.getPersistenceManager(o1)) {
//        		throw DBLogger.newUser("The object belongs to another PersistenceManager");
//        	}
//    		while (!cls.isAssignableFrom(cls2)) {
//    			cls = cls.getSuperclass();
//    		}
//    	}
//		ext = new ;
//		setClass(ZooPC.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void setClass(Class cls) {
		checkUnmodifiable();
    	if (!ZooPC.class.isAssignableFrom(cls)) {
    		throw DBLogger.newUser("Class is not persistence capabale: " + cls.getName());
    	}
		candCls = cls;
		Node node = pm.getSession().getPrimaryNode();
		ZooClassProxy sch = pm.getSession().getSchemaManager().locateSchema(cls, node);
		if (sch != null) {
			candClsDef = sch.getSchemaDef();
		} else {
    		if (pm.getSession().getConfig().getAutoCreateSchema()) {
    			isDummyQuery = true;
    		} else {
    			throw DBLogger.newUser("Class schema is not defined: " + cls.getName());
    		}
		}
	}

	@SuppressWarnings("rawtypes")
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
		resetQuery();
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
	public void setOrdering(String orderingString) {
		checkUnmodifiable();
		if (orderingString != null && orderingString.trim().length() == 0) {
			orderingStr = null;
		} else {
			orderingStr = orderingString;
		}
		resetQuery();
	}

	@Override
	public void setRange(String fromInclToExcl) {
		rangeMin = 0;
		rangeMax = Long.MAX_VALUE;
		rangeStr = fromInclToExcl;
		if (rangeStr != null) {
			rangeStr = rangeStr.trim();
			if (rangeStr.length() == 0) {
				rangeStr = null;
			}
		}
		//For string-range, we have to reset the query. The range may contain parameters.
		resetQuery();
	}

	@Override
	public void setRange(long fromIncl, long toExcl) {
		if (fromIncl < 0 || fromIncl > toExcl) {
			throw DBLogger.newUser("Illegal range argument: " + fromIncl + " / " + toExcl);
		}
		if (rangeStr != null) {
			resetQuery();
		}
		rangeStr = null;
		rangeMin = fromIncl;
		rangeMax = toExcl;
	}

	@Override
	public void setResult(String data) {
		checkUnmodifiable();
		//we can't check this here, because the filter and candidate class may still change
		resultSettings = data;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setResultClass(Class cls) {
		checkUnmodifiable();
		resultClass = cls;
	}

	@Override
	public void setUnique(boolean unique) {
		checkUnmodifiable();
		this.unique = unique;
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

	@Override
	public void cancel(Thread arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public void cancelAll() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public Integer getDatastoreReadTimeoutMillis() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Boolean getSerializeRead() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void setDatastoreReadTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public void setSerializeRead(Boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public String toString() {
		return "Filter: \"" + filter + "\"   -----  Tree: " + 
				queryTree != null ? queryTree.print() : "not compiled";
	}
	
}
