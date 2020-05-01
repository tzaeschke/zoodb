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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.testutil.rmi.RmiTaskRunner.NoSecurityManager;

public class RmiTestProcess implements RmiTestProcessI {

	@Override
	public void executeTask(RmiTestTask task) {
		try {
			task.test();
		} finally {
			try {
				UnicastRemoteObject.unexportObject(this, true);
			} catch (NoSuchObjectException e) {
				throw DBLogger.wrap(e);
			}
		}
    }
	
	@Override
	public boolean isAlive() {
		return true;
	}
	
	public static void main(String[] args) {
		//System.out.println("TestRunnerLocal: started: " + args[0] + " " + args[1] + " " + args[2]);
		//System.out.println("TestRunnerLocal time: " + new Date());
		
		System.setSecurityManager (new NoSecurityManager());
        try {
        	RmiTestProcess engine = new RmiTestProcess();
            RmiTestProcessI stub =
                (RmiTestProcessI) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry();
            System.err.println("TestRunnerLocal bound");
            registry.rebind(RmiTaskRunner.RMI_NAME, stub);
            System.err.println("TestRunnerLocal bound");
        } catch (Exception e) {
            System.err.println("TestRunnerLocal exception:");
            throw DBLogger.wrap(e);
        }
		
		//System.out.println("TestRunnerLocal finished: " + args[0]);
	}

}
