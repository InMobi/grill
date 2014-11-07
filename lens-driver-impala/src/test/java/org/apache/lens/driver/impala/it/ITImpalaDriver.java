package org.apache.lens.driver.impala.it;

/*
 * #%L
 * Grill Driver for Cloudera Impala
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.GrillException;
import org.apache.lens.driver.impala.ImpalaDriver;
import org.apache.lens.driver.impala.ImpalaResultSet;


public class ITImpalaDriver {

	@Test
	public void testIntegration() throws GrillException {
		ImpalaDriver iDriver = new ImpalaDriver();
		Configuration config = new Configuration();
		config.set("PORT", "21000");
		config.set("HOST", "localhost");
		iDriver.configure(config);
		
			List<Object> row = null;
			ImpalaResultSet iResultSet = (ImpalaResultSet) iDriver
					.execute("select * from emp", null);
			if (iResultSet.hasNext()) {
				row = iResultSet.next().getValues();
				System.out.println("Row1" + row);
			}
			if (iResultSet.hasNext()) {
				row = iResultSet.next().getValues();
				System.out.println("Row2" + row);
			}
			Assert.assertTrue(true);
		
	}
	
	
}
