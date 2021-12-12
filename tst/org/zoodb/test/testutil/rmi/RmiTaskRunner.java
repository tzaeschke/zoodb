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
package org.zoodb.test.testutil.rmi;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.Permission;

import org.zoodb.internal.util.DBLogger;

public class RmiTaskRunner {

	private static final boolean USE_RMI = true;
	static final String RMI_NAME = "ZooDBTestRunnerRMI";
	private static final int RMI_PORT = 1099;
	
	static class NoSecurityManager extends SecurityManager {
	    @Override
		public void checkConnect (String host, int port) {}
	    @Override
		public void checkConnect (String host, int port, Object context) {}
	    @Override
		public void checkPropertyAccess(String key) {}

		@Override
		public void checkPermission(Permission perm) {}

		@Override
		public void checkPermission(Permission perm, Object context) {}

		@Override
		public void checkAccept(String host, int port) {}
	}

	public static void executeTask(RmiTestTask task) {
		if (USE_RMI) {
			runRmiTest(task);
		} else {
			task.test();
		}
	}

	private static void runRmiTest(RmiTestTask task) {
		//start registry
		Registry registry;
		try {
			registry = LocateRegistry.createRegistry(RMI_PORT);
		} catch (RemoteException e) {
			//exists???
			e.printStackTrace();
			try {
				registry = LocateRegistry.getRegistry();
			} catch (RemoteException e2) {
				e2.printStackTrace();
				throw DBLogger.wrap(e2);
			}
		}

		//Check whether there are already tests running
		try {
			RmiTestProcess comp = (RmiTestProcess) registry.lookup(RMI_NAME);
			if (comp.isAlive()) {
				throw new IllegalStateException("Test process already running!");
			}
		} catch (NotBoundException e) {
			//good! -> free!
		} catch (RemoteException e) {
			throw DBLogger.wrap(e);
		}
		
		
		// start test process
		System.out.println("Manager: starting task.");
		System.setSecurityManager (new NoSecurityManager());
		Process p = TestProcessLauncher.launchProcess("", RmiTestProcess.class, new String[]{});

		RmiTestProcessI comp = null;
		for (int i = 0; i < 10; i++) {
			try {
				Thread.sleep(100);
				comp = (RmiTestProcessI) registry.lookup(RMI_NAME);
			} catch (InterruptedException e) {
				throw DBLogger.wrap(e);
			} catch (RemoteException e) {
				throw DBLogger.wrap(e);
			} catch (NotBoundException e) {
				//ignore, retry
			}
		}
		
		if (comp == null) {
			throw new IllegalStateException("Could not find remote process.");
		}
		
		// run test
		try {
			comp.executeTask(task);
			Thread.sleep(100);
		} catch (RemoteException | InterruptedException e) {
			throw DBLogger.wrap(e);
		} finally {
			//end process
			p.destroy();

			//wait for end
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				throw DBLogger.wrap(e);
			}
			try {
				UnicastRemoteObject.unexportObject(registry, true);
			} catch (NoSuchObjectException e) {
				throw DBLogger.wrap(e);
			}
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
