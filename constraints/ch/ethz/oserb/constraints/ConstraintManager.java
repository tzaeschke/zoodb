package ch.ethz.oserb.constraints;

import javax.jdo.PersistenceManager;
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.LoadLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import org.zoodb.jdo.ex1.ExamplePerson;

/*import org.eclipse.ocl.OCL;
import org.eclipse.ocl.ecore.EcoreEnvironmentFactory;
import org.eclipse.ocl.helper.OCLHelper;
import org.zoodb.jdo.ex1.ExamplePerson;*/

public class ConstraintManager {
	private PersistenceManager pm;
	/**
	 * constructor.
	 * @param PersistenceManager
	 */
	public ConstraintManager(PersistenceManager pm){
		// register listeners
		pm.addInstanceLifecycleListener(new ListenerClear(), ExamplePerson.class);
		pm.addInstanceLifecycleListener(new ListenerCreate(), ExamplePerson.class);
		pm.addInstanceLifecycleListener(new ListenerDelete(), ExamplePerson.class);
		pm.addInstanceLifecycleListener(new ListenerDirty(), ExamplePerson.class);
		pm.addInstanceLifecycleListener(new ListenerLoad(), ExamplePerson.class);
		pm.addInstanceLifecycleListener(new ListenerStore(), ExamplePerson.class);
		
		// create an OCL instance & helper
		/*OCL ocl = OCL.newInstance(EcoreEnvironmentFactory.INSTANCE);
		OCLHelper<EClassifier, ?, ?, Constraint> oclHelper = ocl.createOCLHelper();*/
		
		
		this.pm = pm;
	}
	public boolean checkConstraints(){
		
		return true;
	}
	private static class ListenerLoad implements LoadLifecycleListener {
		@Override
		public void postLoad(InstanceLifecycleEvent arg0) {
			System.out.println("postLoad");		
		}
	}
	
	private static class ListenerStore implements StoreLifecycleListener {
		@Override
		public void postStore(InstanceLifecycleEvent arg0) {
			System.out.println("postStore");			
		}
		@Override
		public void preStore(InstanceLifecycleEvent arg0) {
			System.out.println("preStore");		
		}
	}
	
	private static class ListenerClear implements ClearLifecycleListener {
		@Override
		public void postClear(InstanceLifecycleEvent arg0) {
			System.out.println("postClear");			
		}
		@Override
		public void preClear(InstanceLifecycleEvent arg0) {
			System.out.println("preClear");		
		}
	}
	
	private static class ListenerDelete implements DeleteLifecycleListener {
		@Override
		public void postDelete(InstanceLifecycleEvent arg0) {
			System.out.println("postDelete");		
		}
		@Override
		public void preDelete(InstanceLifecycleEvent arg0) {
			System.out.println("preDelete");			
		}
	}
	
	private static class ListenerCreate implements CreateLifecycleListener {
		@Override
		public void postCreate(InstanceLifecycleEvent arg0) {
			System.out.println("postCreate");			
		}
	}
	
	private static class ListenerDirty implements DirtyLifecycleListener {
		@Override
		public void postDirty(InstanceLifecycleEvent arg0) {
			System.out.println("postDirty");		
		}
		@Override
		public void preDirty(InstanceLifecycleEvent arg0) {
			System.out.println("preDirty");		
		}
	}
}
/*		// managed objects: filter out class defs
		Set mobs = pm.getManagedObjects();
		int i= 0;
		Iterator itr = mobs.iterator();
		while(itr.hasNext()){
			Object obj = itr.next();
			if(obj.getClass().equals(ZooClassDef.class))continue;
			if(((ZooPCImpl)obj).jdoZooIsDirty()) i++;
		}
		System.out.println("dirty: "+i);
*/

/*		// managed objects: filter class
		Set mobs = pm.getManagedObjects();
		int i= 0;
		System.out.println(mobs.size());
		Iterator itr = mobs.iterator();
		while(itr.hasNext()){
			Object elem = itr.next();
			if(elem.getClass().equals(extent)){
				ZooPCImpl p = extent.cast(elem);
				if(p.jdoZooIsDirty())i++;
			}
		}
		System.out.println("size: "+i);
*/