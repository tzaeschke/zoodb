/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.testutil;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.zoodb.test.testutil.RmiTestRunner.NoSecurityManager;


public class RmiTaskLauncher {

	private static final boolean USE_RMI = true;
	
	static final String RMI_NAME = "ZooDbRmiTestRunner";
	
	
	private static void runRmiTest(RmiTestTask task) {
		// start test process
		System.setSecurityManager (new NoSecurityManager());
		TestProcess p = TestProcess.launchProcess("", //"-Xmx28G -XX:+UseConcMarkSweepGC", 
				RmiTestRunner.class, new String[]{task.getClass().getName()});

		// run test
		int n = 0;
		while (true) {
			try {
				Registry registry = LocateRegistry.getRegistry();
				RmiTestRunnerAPI comp = (RmiTestRunnerAPI) registry.lookup(RMI_NAME);
				comp.executeTask(task);
			} catch (AccessException e) {
				throw new RuntimeException(e);
			} catch (RemoteException e) {
				throw new RuntimeException(e);
			} catch (NotBoundException e) {
				if (n++ < 10) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e2) {
						throw new RuntimeException(e2);
					}
					continue;
				}
				throw new RuntimeException(e);
			} finally {
				//end process
				p.stop();
			}
			break;
		}
	}

	
	public static void runTest(RmiTestTask task) {
		if (USE_RMI) {
			runRmiTest(task);
		} else {
			task.test();
		}
	}
	
	
}
