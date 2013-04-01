package org.zoodb.profiling.simulator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;


/**
 * This class simulates a group of users. Users can be run concurrently or sequentially.
 * 
 * @author tobiasg
 *
 */
public abstract class UserSimulator {
	
	/**
	 * Number of users we want to simulate 
	 */
	private int numberOfUsers = 1;
	
	/**
	 * Do users run concurrently or sequentially?
	 * Do not modify this member after instantiation!
	 */
	boolean executeConcurrent = false;
	
	private ActionArchive actions;
	
	public UserSimulator(int numberOfUsers, boolean executeConcurrent) {
		this.numberOfUsers = numberOfUsers;
		this.executeConcurrent = executeConcurrent;
	}
	
	public void run() {
		ExecutorService threadPool = Executors.newFixedThreadPool(numberOfUsers);
		
		Set<Future<Object>> fus = new HashSet<Future<Object>>();
		List<Object> results = new LinkedList<Object>();
		
		
		
		PersistenceManager pm = getPMF().getPersistenceManager();
		
		//enable profiling
		init();
		
		// start user threads
		for (int i=0; i<numberOfUsers;i++) {
			User<Object> u = new ZooDBUser<Object>(pm,actions);
			
			Future<Object> f = threadPool.submit(u);
			fus.add(f);
			
			/*
			 * f.get() will block
			 * Next user will only be created when the previous one has finished
			 */
			if (!executeConcurrent) {
				try {
					results.add(f.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		
		/*
		 * Collect values if users were simulated concurrently
		 */
		if(executeConcurrent) {
			for (Future<?> f : fus) {
				try {
					results.add(f.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		
		threadPool.shutdown();
		
		shutdown();
	}
	
	
	/**
	 * Bootstraps database, creates PersistenceManager(factory) etc.
	 * Subclasses can use this method to start a profiler
	 */
	protected abstract void init();
	
	protected abstract void shutdown();
	
	protected abstract PersistenceManagerFactory getPMF();

	public ActionArchive getActions() {
		return actions;
	}

	public void setActions(ActionArchive actions) {
		this.actions = actions;
	}

}
