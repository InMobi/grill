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



import org.apache.lens.cli.commands.GrillStorageCommands;
import org.apache.lens.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URL;

public class TestGrillStorageCommands extends GrillCliApplicationTest {

  private static GrillStorageCommands command;
  private static final Logger LOG = LoggerFactory.getLogger(TestGrillStorageCommands.class);


  @Test
  public void testStorageCommands() {
    addLocalStorage("local_storage_test");
    testUpdateStorage("local_storage_test");
    dropStorage("local_storage_test");

  }

  private static GrillStorageCommands getCommand() {
    if(command == null) {
      GrillClient client = new GrillClient();
      command = new GrillStorageCommands();
      command.setClient(client);
    }
    return command;
  }

  public static void dropStorage(String storageName) {
    String storageList;
    GrillStorageCommands command = getCommand();
    command.dropStorage(storageName);
    storageList = command.getStorages();
    Assert.assertFalse( storageList.contains(storageName),"Storage list contains "+storageName);
  }

  public synchronized static void addLocalStorage(String storageName) {
    GrillStorageCommands command = getCommand();
    URL storageSpec =
        TestGrillStorageCommands.class.getClassLoader().getResource("local-storage.xml");
    File newFile = new File("/tmp/local-"+storageName+".xml");
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();

      xmlContent = xmlContent.replace("name=\"local\"",
          "name=\""+storageName+"\"");

      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();
    LOG.debug("Using Storage spec from file : " + newFile.getAbsolutePath());
    String storageList = command.getStorages();
    Assert.assertFalse(storageList.contains(storageName),
        " Storage list contains "+storageName + " storage list is  "
            + storageList + " file used is " + newFile.getAbsolutePath());
    command.createStorage(newFile.getAbsolutePath());
    storageList = command.getStorages();
    Assert.assertTrue(storageList.contains(storageName));
    } catch (Exception e) {
      Assert.fail("Unable to add storage " + storageName);
    } finally {
      newFile.delete();
    }
  }

  private void testUpdateStorage(String storageName) {

    try {
      GrillStorageCommands command = getCommand();
      URL storageSpec =
          TestGrillStorageCommands.class.getClassLoader().getResource("local-storage.xml");
      StringBuilder sb = new StringBuilder();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(storageSpec.getFile()));
      String s;
      while ((s = bufferedReader.readLine()) != null) {
        sb.append(s).append("\n");
      }

      bufferedReader.close();

      String xmlContent = sb.toString();
      xmlContent = xmlContent.replace("name=\"local\"",
          "name=\""+storageName+"\"");
      xmlContent = xmlContent.replace("<properties name=\"storage.url\" value=\"file:///\"/>\n",
          "<properties name=\"storage.url\" value=\"file:///\"/>" +
              "\n<properties name=\"sample_cube.prop1\" value=\"sample1\" />\n");

      File newFile = new File("/tmp/"+storageName+".xml");
      Writer writer = new OutputStreamWriter(new FileOutputStream(newFile));
      writer.write(xmlContent);
      writer.close();

      String desc = command.describeStorage(storageName);
      LOG.debug(desc);
      String propString = "name : storage.url  value : file:///";
      Assert.assertTrue(desc.contains(propString));

      command.updateStorage(storageName+" /tmp/local-storage1.xml");
      desc = command.describeStorage(storageName);
      LOG.debug(desc);
      Assert.assertTrue(desc.contains(propString));
      newFile.delete();

    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Testing update storage failed with exception" + t.getMessage());
    }
  }


}
