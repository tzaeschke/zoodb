package ch.ethz.oserb.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ test_foreign.class, test_ocl_annotation.class,
		test_ocl_configurer.class, test_primary.class, test_profiles.class,
		test_severity.class, test_unique.class, test_factory.class, test_pojo.class })
public class AllTests {

}
