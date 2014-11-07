package org.apache.lens.cli;
/*
 * #%L
 * Grill CLI
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



import org.apache.lens.cli.commands.GrillConnectionCommands;
import org.apache.lens.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class TestGrillConnectionCliCommands extends GrillCliApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestGrillConnectionCliCommands.class);


  @Test
  public void testClientCreation() {
    GrillClient client = null;
    try {
      client = new GrillClient();
    } catch (Throwable t) {
      Assert.fail("Client should have been able to create a connection to server");
    } finally {
      if (client != null)
        client.closeConnection();
    }
  }

  @Test
  public void testConnectionCommand() {
    GrillClient client = new GrillClient();
    GrillConnectionCommands commands = new GrillConnectionCommands();
    commands.setClient(client);
    String key = "connectiontest1";
    String value = "connectiontest1val";
    String keyvalList = commands.showParameters();

    Assert.assertFalse(keyvalList.contains("connectiontest1"));

    commands.setParam(key + "=" + value);
    String val = commands.getParam(key);
    Assert.assertEquals(val, key + "=" + value);
    commands.quitShell();
  }

  @Test
  public void testFileCommands() {
    GrillClient client = new GrillClient();
    GrillConnectionCommands commands = new GrillConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    String filename = "/tmp/data";
    File f = new File(filename);
    try {
      f.createNewFile();
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    String result = commands.addFile(filename);
    Assert.assertEquals("Add resource succeeded", result);

    result = commands.removeFile(filename);
    Assert.assertEquals("Delete resource succeeded", result);
    LOG.debug("Testing set/remove file operation done");
    f.delete();
    commands.quitShell();
  }


  @Test
  public void testJarCommands() {
    GrillClient client = new GrillClient();
    GrillConnectionCommands commands = new GrillConnectionCommands();
    commands.setClient(client);
    LOG.debug("Testing set/remove file operations");

    String filename = "/tmp/data.jar";
    File f = new File(filename);
    try {
      f.createNewFile();
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    String result = commands.addJar(filename);
    Assert.assertEquals("Add resource succeeded", result);

    result = commands.removeJar(filename);
    Assert.assertEquals("Delete resource succeeded", result);
    LOG.debug("Testing set/remove file operation done");
    f.delete();
    commands.quitShell();
  }
}
