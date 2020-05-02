/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.jdo.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.query.QueryExecutor;
import org.zoodb.internal.query.ParameterDeclaration;
import org.zoodb.internal.query.ParameterDeclaration.DECLARATION;
import org.zoodb.internal.query.QueryParser;
import org.zoodb.internal.query.QueryParserAPI;
import org.zoodb.internal.query.QueryParserV3;
import org.zoodb.internal.query.QueryParserV4;
import org.zoodb.internal.query.QueryTree;
import org.zoodb.internal.query.QueryVariable;
import org.zoodb.internal.query.QueryVariable.VarDeclaration;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.ObjectIdentitySet;
import org.zoodb.internal.util.Pair;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.DBStatistics.STATS;


/**
 * Query implementation.
 * 
 * @author Tilmann Zaeschke
 */
public class QueryImpl implements Query {

	public static final Logger LOGGER = LoggerFactory.getLogger(QueryImpl.class);

	private enum EXECUTION_TYPE {
		V3,
		V4,
		FORCED_V3,
		FORCED_V4,
		UNDEFINED;
	}

	/**
	 * Set this to FORCED_V4 if you encounter query parsing problems.
	 */
	public static boolean ENFORCE_QUERY_V4 = false;
	private EXECUTION_TYPE executionType =
			ENFORCE_QUERY_V4 ? EXECUTION_TYPE.FORCED_V4 : EXECUTION_TYPE.UNDEFINED;

	public static boolean USE_V4 = true;
	public static final Object[] NO_PARAMS = new Object[] {};

	/** default. */
	private static final long serialVersionUID = 1L;

	// transient to satisfy findbugs (Query is Serializable, but _pm / _ext are not).
	private transient PersistenceManagerImpl pm;
	private transient Extent<?> ext;
	private boolean isUnmodifiable = false;
	private Class<?> candCls = ZooPC.class; //TODO good default?
	private transient ZooClassDef candClsDef = null;
	private String filter = "";
	
	private boolean unique = false;
	private boolean subClasses = true;
	private boolean ignoreCache = true;
	private ArrayList<Pair<ZooFieldDef, Boolean>> ordering = new ArrayList<>();
	private String orderingStr = null;
	
	private String resultSettings = null;
	private Class<?> resultClass = null;
	
	private final ObjectIdentitySet<Object> queryResults = new ObjectIdentitySet<>();

	private ArrayList<ParameterDeclaration> parameters = new ArrayList<>();
	private ArrayList<QueryVariable> variables = new ArrayList<>();
	
	private QueryTree queryTree;
	private QueryExecutor queryExecutor;
	//This is used in schema auto-create mode when the persistent class has no schema defined
	private boolean isDummyQuery = false;
	
	private long rangeMin = 0;
	private long rangeMax = Long.MAX_VALUE;
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
	 * @param pm The PersistenceManager
	 * @param filter The filter
	 */
	public QueryImpl(PersistenceManagerImpl pm, String filter) {
	    this(pm);
	    
	    if (filter == null || filter.length() == 0) {
	    	throw new NullPointerException("Please provide a query string.");
	    }
	    StringTokenizer st = new StringTokenizer(filter);
	    
		String q = filter.trim();
		String tok = st.nextToken();

		//SELECT
		if (!tok.toLowerCase().equals("select")) {
			throw new JDOUserException("Illegal token in query: \"" + tok + "\"");
		}
		q = q.substring(6).trim();
		tok = st.nextToken();

		//UNIQUE
		//TODO this is all covered in the parser V4, except setting UNIQUE=true...
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
			return;
		}
		if (queryResult instanceof ExtentAdaptor) {
			((ExtentAdaptor<?>)queryResult).closeAll();
		} else if (queryResult instanceof ExtentImpl) {
			((ExtentImpl<?>)queryResult).closeAll();
		} else {
			//TODO ignore this
			LOGGER.warn("QueryResult not closable.");
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
		
		//TODO do we really need this?
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
		
		executionType = determineExecutionType();

		QueryParserAPI qp;
		//We do this on the query before assigning values to parameter.
		//Would it make sense to assign the values first and then properly parse the query???
		//Probably not: 
		//- every parameter change would require rebuilding the tree
		//- we would require an additional parser to assign the parameters
		//QueryParser qp = new QueryParser(filter, candClsDef, parameters, ordering);
		//QueryParserV2 qp = new QueryParserV2(filter, candClsDef, parameters, ordering); 
		//QueryParserV3 qp =
		//		new QueryParserV3(fStr, candClsDef, parameters, variables, ordering, rangeMin, rangeMax);
		if (executionType == EXECUTION_TYPE.V4 || executionType == EXECUTION_TYPE.FORCED_V4 ) {
			qp = new QueryParserV4(fStr, candClsDef, parameters, variables,
					ordering, rangeMin, rangeMax, pm.getSession());
		} else {
			qp = new QueryParserV3(fStr, candClsDef, parameters, ordering, rangeMin, rangeMax);
		}
		queryTree = qp.parseQuery();
		rangeMin = qp.getRangeMin();
		rangeMax = qp.getRangeMax();
	}
	
	private void resetQuery() {
		//See Test_122: We need to clear this for setFilter() calls
		for (int i = 0; i < parameters.size(); i++) {
			ParameterDeclaration p = parameters.get(i);
			if (p.getDeclaration() != DECLARATION.API) {
				parameters.remove(i);
				i--;
			}
		}
		queryTree = null;
		queryExecutor = null;
		ordering.clear();
		if (executionType == EXECUTION_TYPE.V3 || executionType == EXECUTION_TYPE.V4) {
			executionType = EXECUTION_TYPE.UNDEFINED;
		}
	}

	@Override
	public void declareImports(String imports) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * For example:
	 * Query q = pm.newQuery (Employee.class, "salary = sal _AND_ name.startsWith(begin");
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
			paramString = paramString.substring(i1+1).trim();
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
		for (ParameterDeclaration p: parameters) {
			if (p.getName().equals(name)) {
				throw new JDOUserException("Duplicate parameter name: " + name);
			}
		}
		Class<?> cls = QueryParser.locateClassFromShortName(type);
		ParameterDeclaration p = new ParameterDeclaration(cls, name, DECLARATION.API,
				parameters.size());
		parameters.add(p);
		if (ZooPC.class.isAssignableFrom(cls)) {
			ZooClassDef typeDef;
			typeDef = pm.getSession().getSchemaManager().locateSchema(cls, null).getSchemaDef();
			p.setTypeDef(typeDef);
		}
	}
	
	@Override
	public void declareVariables(String variablesDecl) {
		checkUnmodifiable();
		variables.clear();
		variablesDecl = variablesDecl.trim();
		int i1 = variablesDecl.indexOf(',');
		while (i1 >= 0) {
			String p1 = variablesDecl.substring(0, i1).trim();
			updateVariableDeclaration(p1);
			variablesDecl = variablesDecl.substring(i1+1).trim();
			i1 = variablesDecl.indexOf(',');
		}
		updateVariableDeclaration(variablesDecl);
		resetQuery();
	}

	private void updateVariableDeclaration(String varDecl) {
		int i = varDecl.indexOf(' ');
		String type = varDecl.substring(0, i);
		String name = varDecl.substring(i+1);
		for (QueryVariable p: variables) {
			if (p.getName().equals(name)) {
				throw new JDOUserException("Duplicate variable name: " + name);
			}
		}
		Class<?> cls = QueryParser.locateClassFromShortName(type);
		//id=0 is preserved for the 'global' variable
		int id = variables.size() + 1;
		QueryVariable qv = new QueryVariable(cls, name, VarDeclaration.API, id);
		variables.add(qv);
		if (ZooPC.class.isAssignableFrom(cls)) {
			ZooClassDef typeDef;
			typeDef = pm.getSession().getSchemaManager().locateSchema(cls, null).getSchemaDef();
			qv.setTypeDef(typeDef);
		}
	}

	@Override
	public long deletePersistentAll() {
		checkUnmodifiable(); //?
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
				//We use a separate extent here. 'ext' is only for explicitly provided extents.
				//Otherwise, use of extents depends on whether we define a filter (maybe in a
				//second execution).
				Extent<?> extent = ext;
				if (extent == null) {
					extent = new ExtentImpl(candCls, subClasses, pm, ignoreCache);
				}
				if (DBStatistics.isEnabled()) {
					pm.getSession().statsInc(STATS.QU_EXECUTED_TOTAL);
					pm.getSession().statsInc(STATS.QU_EXECUTED_WITHOUT_INDEX);
				}
				createExecutor();
				return queryExecutor.runWithExtent(new ExtentAdaptor(extent),
						rangeMin, rangeMax, resultSettings, resultClass);
			} finally {
				pm.getSession().unlock();
			}
		}
		
		compileQuery();
		checkParamCount(0);
		return runQuery(NO_PARAMS);
	}


	private void createExecutor() {
		if (queryExecutor == null) {
			queryExecutor = new QueryExecutor(pm.getSession(), filter, candCls, candClsDef,
					unique, subClasses, isDummyQuery, ordering, queryTree, parameters, variables);
		}
	}

	private Object runQuery(Object[] params) {
		createExecutor();
		ParameterDeclaration.adjustValues(parameters, params);
		return queryExecutor.runQuery(ext, rangeMin, rangeMax, resultSettings, resultClass,
				ignoreCache, params);
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
		return runQuery(parameters);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Object executeWithMap(Map parameters) {
		compileQuery();
		Object[] params = new Object[parameters.size()];
		for (ParameterDeclaration p: this.parameters) {
			p.setValue(params, parameters.get(p.getName()));
		}
		checkParamCount(parameters.size());
		return runQuery(params);
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

	private EXECUTION_TYPE determineExecutionType() {
		if (executionType == EXECUTION_TYPE.FORCED_V3
				|| executionType == EXECUTION_TYPE.FORCED_V4) {
			return executionType;
		}
		if (!variables.isEmpty() || resultClass != null || resultSettings != null) {
			return EXECUTION_TYPE.V4;
		}
		if (filter == null || filter.length() == 0) {
			return EXECUTION_TYPE.V3;
		}
		for (int i = 0; i < filter.length(); i++) {
			char c = filter.charAt(i);
			switch (c) {
			case '.':
			case '|':
			case '+':
			case '-':
			case '*':
			case '/':
				return EXECUTION_TYPE.V4;
			}
		}
		return EXECUTION_TYPE.V3;
	}


	@Override
	public String toString() {
		return "Filter: \"" + filter + "\"   -----  Tree: " + 
				queryTree != null ? queryTree.print() : "not compiled";
	}
	
}
