package org.zoodb.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ObjectIdentitySet;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.query.QueryAdvice;
import org.zoodb.jdo.internal.query.QueryOptimizer;
import org.zoodb.jdo.internal.query.QueryParameter;
import org.zoodb.jdo.internal.query.QueryParser;
import org.zoodb.jdo.internal.query.QueryTerm;
import org.zoodb.jdo.internal.query.QueryTreeIterator;
import org.zoodb.jdo.internal.query.QueryTreeNode;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.CloseableIterator;
import org.zoodb.jdo.stuff.DatabaseLogger;


/**
 * Query implementation.
 * 
 * @author Tilmann Zäschke
 */
public class QueryImpl implements Query {

	/** default. */
	private static final long serialVersionUID = 1L;

	// transient to satisfy findbugs (Query is Serializable, but _pm / _ext are not).
	private transient PersistenceManagerImpl _pm;
	private transient Extent<?> _ext;
	private boolean _isUnmodifiable = false;
	private Class<?> _candCls = PersistenceCapableImpl.class; //TODO good default?
	private transient ZooClassDef _candClsDef = null;
	private List<QueryAdvice> _indexToUse = null;
	private String _filter = "";
	
	private boolean _unique = false;
	private boolean _subClasses = true;
	private boolean _ascending = true;

	private List<QueryParameter> _parameters = new LinkedList<QueryParameter>();
	public QueryImpl(PersistenceManagerImpl pm, Extent ext, String filter) {
		_pm = pm;
		_ext = ext;
		setClass( _ext.getCandidateClass() );
		_filter = filter;
	}

	public QueryImpl(PersistenceManagerImpl pm, Class cls,
			String arg1) {
		_pm = pm;
		setClass( cls );
		_filter = arg1;
	}

	public QueryImpl(PersistenceManagerImpl pm) {
		_pm = pm;
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
		_pm = pm;
		//TODO use char-sequences?
		String q = arg0.trim();
		String tok = getToken(q);

		//SELECT
		if (!tok.toLowerCase().equals("select")) {
			throw new JDOUserException("Illegal token in query: \"" + tok + "\"");
		}
		q = q.substring(6).trim();
		tok = getToken(q);

		//UNIQUE
		if (tok.toLowerCase().equals("unique")) {
			_unique = true;
			q = q.substring(6).trim();
			tok = getToken(q);
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
			tok = getToken(q);
			setClass( locateClass(tok) );
			q = q.substring(tok.length()).trim();
			tok = getToken(q);

			//EXCLUDE SUBCLASSES
			if (tok.toLowerCase().equals("exclude")) {
				q = q.substring(7).trim();
				tok = getToken(q);
				if (!tok.toLowerCase().equals("subclasses")) {
					throw new JDOUserException("Illegal token in query, expected 'SUBCLASSES': \"" + 
							tok + "\"");
				}
				_subClasses = false;
				q = q.substring(7).trim();
				tok = getToken(q);
			}
		}

		//WHERE
		if (tok.toLowerCase().equals("where")) {
			q = q.substring(5).trim();
			_filter = q;
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

	private String getToken(String q) {
		q = q.trim();
		int i = q.indexOf(' ');
		if (i == -1) {
			return q;
		}
		q = q.substring(0, i);
		return q;
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
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void closeAll() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void compile() {
		checkUnmodifiable(); //? TODO ?

		//We do this on the query before assigning values to parameter.
		//Would it make sense to assign the values first and then properly parse the query???
		//Probably not: 
		//- every parameter change would require rebuilding the tree
		//- we would require an additional parser to assign the parameters
		QueryParser qp = new QueryParser(_filter, _candClsDef); 
		QueryTreeNode queryTree = qp.parseQuery();

		assignParametersToQueryTree(queryTree);
		//This is only for indices, not for given extents
		QueryOptimizer qo = new QueryOptimizer(_candClsDef);
		_indexToUse = qo.determineIndexToUse(queryTree);
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
			_parameters .add(new QueryParameter(p1));
			parameters = parameters.substring(i1+1, parameters.length()).trim();
			i1 = parameters.indexOf(',');
		}
		_parameters .add(new QueryParameter(parameters));
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
		// TODO Auto-generated method stub
		//TODO check if extent is used
		//TODO if query is used, check if it is already executed and re-use result.
		//TODO if not executed (or if extent), implement special call to directly delete objects
		//     before they are materialized. In that case, run query also on cache to eliminate
		//     loaded objects, even if not in database yet(is this optional?)!
		//     Take care, that cached local versions of stored objects may not match the query
		//     anymore and should probably(?) not be deleted from the database!(???)
		DatabaseLogger.debugPrintln(2, "STUB QueryImpl.deletePersistentAll()");
		Collection<?> c = (Collection<?>) execute();
		int size = 0;
		for (Object o: c) {
		    //TODO this is bad!
			if (!JDOHelper.isDeleted(o)) {
				size++;
			}
		}
		if (!_pm.getIgnoreCache()) {
			DatabaseLogger.debugPrintln(1, 
					"Query.deletePersistentAll(): ignore-cache = false not supported.");
		}
		_pm.deletePersistentAll(c);
		return size;
	}

	@Override
	public long deletePersistentAll(Object... parameters) {
		checkUnmodifiable(); //?
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
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
			for (QueryParameter param: _parameters) {
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
		Iterator<?> ext;
		if (qa.getIndex() != null) {
			//TODO other nodes...
			ext = _pm.getSession().getPrimaryNode().readObjectFromIndex(qa.getIndex(),
					qa.getMin(), qa.getMax());
//			System.out.println("Index: " + qa.getIndex().getName() + "  " + qa.getMin() + "/" + qa.getMax());
		} else {
			//use extent
			if (_ext != null) {
				//use user-defined extent
				ext = _ext.iterator();
			} else {
				//create type extent
				ext = new ExtentImpl(_candCls, _subClasses, _pm).iterator();
			}
		}
		
		if (_ext != null && (!_ext.hasSubclasses() || !_ext.getCandidateClass().isAssignableFrom(_candCls))) {
			final boolean hasSub = _ext.hasSubclasses();
			final Class supCls = _ext.getCandidateClass();
			// normal iteration
			while (ext.hasNext()) {
				Object o = ext.next();
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
			while (ext.hasNext()) {
				Object o = ext.next();
				boolean isMatch = queryTree.evaluate(o);
				if (isMatch) {
					ret.add(o);
				}
			}
		}
		if (ext instanceof CloseableIterator) {
			((CloseableIterator)ext).close();
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object execute() {
		//no go through extent. Skip this if extent was generated on server from local filters.
		
		if (_filter.equals("")) {
	        if (_ext == null) {
	            _ext = new ExtentImpl(_candCls, _subClasses, _pm);
	        }
			return new ExtentAdaptor(_ext);
		}
		
		compile();

		//TODO can also return a list with (yet) unknown size. In that case size() should return
		//Integer.MAX_VALUE (JDO 2.2 14.6.1)
		ArrayList<Object> ret = new ArrayList<Object>();
		for (QueryAdvice qa: _indexToUse) {
			applyQueryOnExtent(ret, qa);
		}
		
		//No check if we need to check for duplicates, i.e. if multiple indices were used.
		for (QueryAdvice qa: _indexToUse) {
			if (qa.getIndex() != _indexToUse.get(0).getIndex()) {
				DatabaseLogger.debugPrintln(0, "Merging query results!");
				System.out.println("Merging query results!");
				Set<Object> ret2 = new ObjectIdentitySet<Object>();
				ret2.addAll(ret);
				return ret2;
			}
		}
		
		return ret;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1) {
		_parameters.get(0).setValue(p1);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2) {
		_parameters.get(0).setValue(p1);
		_parameters.get(1).setValue(p2);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1, Object p2, Object p3) {
		_parameters.get(0).setValue(p1);
		_parameters.get(1).setValue(p2);
		_parameters.get(2).setValue(p3);
		return execute();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object executeWithArray(Object... parameters) {
		for (int i = 0; i < parameters.length; i++) {
			_parameters.get(i).setValue(parameters[i]);
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
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return _pm;
	}

	@Override
	public boolean isUnmodifiable() {
		return _isUnmodifiable;
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
    	if (!PersistenceCapable.class.isAssignableFrom(cls)) {
    		throw new JDOUserException("Class is not persistence capabale: " + cls.getName());
    	}
		_candCls = cls;
		Node node = _pm.getSession().getPrimaryNode();
		_candClsDef = _pm.getSession().getSchemaManager().locateSchema(cls, node).getSchemaDef();
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
		_filter = filter;
	}

	@Override
	public void setGrouping(String group) {
		checkUnmodifiable();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setIgnoreCache(boolean ignoreCache) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setOrdering(String ordering) {
		checkUnmodifiable();
		_ascending = true;
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
		QueryResultProcessor result = new QueryResultProcessor(data, _candCls);
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
		_isUnmodifiable = true;
	}

	private void checkUnmodifiable() {
		if (_isUnmodifiable) {
			throw new JDOUserException("This query is unmodifiable.");
		}
	}

}
