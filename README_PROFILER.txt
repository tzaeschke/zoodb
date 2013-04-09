### ZooDB Profiler ###

# Structure: 
 - most of the logic of profiler implemented in packages: org.zoodb.profiling.*
 - other logic directly integrated in existing classes
 - all profiling enhancements to persistent classes are directly attached to ZooPCImpl
 - Since there are quite a few calls intercepted which collect statistics, I would not use binaries from
 this branch for a "productive" application (for the experiments in the report I have used the master branch)

# Important:
Persistence capable classes can still inherit from ZooPCImpl. Although only PersistenceCapableImpl
provides the methods "activateRead(arg)" and "activateWrite(arg)". Classes which you want to profile
should therefore inherit from PersistenceCapableImpl.

# Dependencies
See pom.xml for all dependencies (commons-profiler needs to be compiled and installed first!)

- "mvn package" creates a jar file with all dependencies included
- "mvn test" executes all tests (the test directory was modified, see pom.xml)
- "mvn install" installs it in the local maven repository

# Some notes on performance:

Until a few million activations, the profiler should work just fine, normal 2GB heap is sufficient.

Attempts have been made to reduce the memory consumption, mainly by using the integrated profiler in the JDK, 
the following attempts were found to bring the most benefits: 

 - use ArrayLists over LinkedLists
 - use ArrayLists over HashMaps
 
The next steps to reduce memory consumption would be:

 - Replace all String which indicate attribute names with indices into the corresponding array 
 - Aggregate and Analyze the activation objects as soon as no paths possible anymore:
 	--> Idea: the evictor could mark the activations (thats easy, activations are directly attached to the ZooPCImpl)
 	--> Analyzers could be started periodically and check whether such marked activations are present. If there are any,
 	they can be analyzed and be freed afterward
 - FieldAccess: FieldAccess are saved in the class SimpleFieldAccess which holds only 3 int's. I think an int array
 	of size 3 should reduce the object/class overhead and free a lot more memory: --> there can be millions
 	of these objects!! (maybe we can also save reads and writes in the same int: 16 bits should be more than enough
 	to capture the number of reads and writes on a single persistent objects
 - Regarding the fieldAccess: these are directly attached to an ArrayList in the ActivationObject: maybe some kind
 	bitmap would be better (the amount of SimpleFieldAccess objects in this ArrayList is limited by the number of non-transient
 	attributes a class has...)
 - Multi-threaded analyzers?
 
 
 
 #############################################
 Comments by TZ:
 - commit() and rollback() clears all predecessor info:
    private void evictForProfiling(ZooPCImpl co) {
		co.setPageId(-1);
		co.setActiveAndQueryRoot(false);
		co.setActivationPathPredecessor(null);
		co.setPredecessorField(null);
		co.setActivation(null);
	}


- PC-loading-times are recorded but never used.
- PC-page-IDs are recorded but never used.


- For every object that is activated, it create an activation path.
  Each activation object stores for each field how often it was
  accessed in read- or write-mode.

Activation of a field:
a) When activateRead/Write is called, it checks the accessed field whether
  it is a reference, and tries to set the activationPredecessor in the
  referenced instance to point to the current instance.
b) Then it appears to do the same thing again (handleActivationMessage).
   Plus (setAndSend()) it creates an Activation instance if the object is hollow
   or access come from a query. 
c) Ass a field access to the Activaion's field-access counter.



TODO fix
- It appears that each PC can only store one predecessor, meaning that additional access 
  will not have the correct predecessor set.
  THis is not completely wrong, because the code indicates that only activations
  are stored. 
  What would be nice to have therefore is to complement activation paths with
  actual Navigation-Paths. Navigation paths are not recorded at the moment,
  only Activation paths and attribute access frequency.


TODO clean-up
- Trx.getUniqueID() should return a long i.o. String. Or maybe we can remove 
  this and use ObjectIdentity instead? Use ID only for export...
- Trx timing is never used.
- Subclasses of AbstractActivation seem unnecessary. Especially the size() in 
  CollectionActivation is not used but expensive to calculate.
  
 - OPTIMIZE DataDeSerializer.reportFieldSizeRead()

 