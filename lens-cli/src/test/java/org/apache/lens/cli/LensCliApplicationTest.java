package org.apache.lens.cli;

/*
 * #%L
 * Lens CLI
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

import org.apache.lens.server.LensAllApplicationJerseyTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * The Class LensCliApplicationTest.
 */
public class LensCliApplicationTest extends LensAllApplicationJerseyTest {

  @Override
  protected int getTestPort() {
    return 9999;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("lensapi").build();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
