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
package org.zoodb.jdo.examples;

import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ex1.ExampleMain;
import org.zoodb.jdo.ex2.Example2Main;
import org.zoodb.jdo.ex3.Example3Main;
import org.zoodb.jdo.perf.large.ExamplePerfLargeMain;
import org.zoodb.jdo.perf.query.ExamplePerfQueryMain;

/**
 * Smoke tests for examples and performance tests.
 * 
 * @author Tilmann Zaeschke
 */
public class TestExamples {

    private static PrintStream originalStream = System.out;
    
    @BeforeClass
    public static void beforeCass() {
        System.setOut(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
              // nothing
            }
          }));
    }
    
    @AfterClass
    public static void afterClass() {
        System.setOut(originalStream);
    }
    
    @Test
    public void smokeTestExample1() {
        ExampleMain.main();
    }
    
    @Test
    public void smokeTestExample2() {
        Example2Main.main();
    }
    
    @Test
    public void smokeTestExample3() {
        Example3Main.main();
    }
    
    @Test
    public void smokeTestPerfLarge() {
        ExamplePerfLargeMain.N_MAX = 10_000;
        ExamplePerfLargeMain.N_BATCH_SIZE = 1_000;
        ExamplePerfLargeMain.main();
    }
    
    @Test
    public void smokeTestPerfQuery() {
        ExamplePerfQueryMain.N_QUERY_AGE = 1000;
        ExamplePerfQueryMain.N_QUERY_AGE_RANGE = 1000;
        ExamplePerfQueryMain.main();
    }
}
