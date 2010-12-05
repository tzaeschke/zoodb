package org.zoodb.jdo;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.QueryParser.QueryTerm;
import org.zoodb.jdo.QueryParser.QueryTreeIterator;
import org.zoodb.jdo.QueryParser.QueryTreeNode;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class QueryImpl implements Query {

	/** default. */
	private static final long serialVersionUID = 1L;

	private PersistenceManagerImpl _pm;
	private Extent<?> _ext;
	private boolean _isUnmodifiable = false;
	private Class<?> _candCls = PersistenceCapableImpl.class; //TODO good default?
	private String _filter = "";
	private QueryTreeNode _queryTree = null;
	// The minimum class required for a query to compile.
	//All candidate objects have to be instances of this class or of a sub-class.
	private Class<?> _minCandCls = PersistenceCapableImpl.class;
	
	private boolean _unique = false;
	private boolean _subClasses = true;
	private boolean _ascending = true;

	private List<QueryParameter> _parameters = new LinkedList<QueryParameter>();
	static class QueryParameter {
		private final String _type;
		private final String _name;
		private Object _value;
		public QueryParameter(String parameter) {
			//TODO split manually i.o. RegEx?
			String[] res = parameter.split(" ");
			_type = res[0];
			_name = res[1];
		}
		public void setValue(Object p1) {
			_value = p1;
		}
		public Object getValue() {
			return _value;
		}

	}

	public QueryImpl(PersistenceManagerImpl pm, Extent ext) {
		_pm = pm;
		_ext = ext;
		setClass( _ext.getCandidateClass() );
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
		buildQueryTree();
		assignParametersToQueryTree();
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
		throw new UnsupportedOperationException();
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

	private void buildQueryTree() {
		//We do this on the query before assigning values to parameter.
		//Would it make sense to assign the values first and then properly parse the query???
		//Probably not: 
		//- every parameter change would require rebuilding the tree
		//- we would require an additional parser to assign the parameters
		QueryParser qp = new QueryParser(_filter, _candCls, _candidateFields.get(_candCls)); 
		_queryTree = qp.parseQuery();
		_minCandCls = qp.getMinRequiredClass();
	}

	private void assignParametersToQueryTree() {
		QueryTreeIterator iter = _queryTree.termIterator();
		while (iter.hasNext()) {
			QueryTerm term = iter.next();
			if (!term.isParametrized()) {
				continue;
			}
			String pName = term.getParamName();
			//TODO cache Fields in QueryTerm to avoid String comparison?
			boolean isAssigned = false;
			for (QueryParameter param: _parameters) {
				if (pName.equals(param._name)) {
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
	
	private List<Object> applyQueryOnExtent() {
		//TODO can also return a list with (yet) unknown size. In that case size() should return
		//Integer.MAX_VALUE (JDO 2.2 14.6.1)
		LinkedList<Object> ret = new LinkedList<Object>();
		for (Object o: _ext) {
			boolean isMatch = _queryTree.evaluate(o);
			if (isMatch) {
				ret.add(o);
			}
		}
		//TODO set field access to false afterwards!
		return ret;
	}
	
	private Map<Class<?>, Map<String, Field>> _candidateFields = 
		new HashMap<Class<?>, Map<String, Field>>();  //TODO use identity set?
	
	private void getCandidateFields(Class<?> cls) {
		//get super classes
		if (cls.getSuperclass() != PersistenceCapableImpl.class) {
			getCandidateFields(cls.getSuperclass());
		}
		
		try {
			Map<String, Field> fields = new HashMap<String, Field>();
			if (cls.getSuperclass() != PersistenceCapableImpl.class) {
				fields.putAll(_candidateFields.get(cls.getSuperclass()));
			}
			//adding the local fields after the super-fields overwrites possible fields with the
			//same name in any superclass.
			for (Field f: cls.getDeclaredFields()) {
				f.setAccessible(true);
				fields.put(f.getName(), f);
			}
			_candidateFields.put(cls, fields);
		} catch (IllegalArgumentException e) {
			throw new JDOFatalUserException("Field not readable: " + cls.getName(), e);
		} catch (SecurityException e) {
			throw new JDOFatalUserException("Field not accessible: " + cls.getName(), e);
		}
	}
	
	@Override
	public Object execute() {
		//no go through extent. Skip this if extent was generated on server from local filters.
		//TODO
		
		if (_filter.equals("")) {
			return new ExtentAdaptor(_ext);
		}
		
		compile();

		if (_ext == null) {
			_ext = new ExtentImpl(_candCls, _subClasses, _pm, _minCandCls);
		}

		return applyQueryOnExtent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object execute(Object p1) {
		//no go through extent. Skip this if extent was generated on server from local filters.
		//TODO

		//TODO move the following into an iterator and the return it
		QueryParameter qp1 = _parameters.get(0);
		qp1.setValue(p1);

		compile();

		if (_ext == null) {
			_ext = new ExtentImpl(_candCls, _subClasses, _pm, _minCandCls);
		}
		
		return applyQueryOnExtent();
	}

	@Override
	public Object execute(Object p1, Object p2) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object execute(Object p1, Object p2, Object p3) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object executeWithArray(Object... parameters) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

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
		_candCls = cls;
		_candidateFields.clear();
		getCandidateFields(cls);
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
