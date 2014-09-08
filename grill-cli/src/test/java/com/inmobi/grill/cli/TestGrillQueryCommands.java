package com.inmobi.grill.cli;


import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.cli.commands.GrillCubeCommands;
import com.inmobi.grill.cli.commands.GrillQueryCommands;
import com.inmobi.grill.client.GrillClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;

public class TestGrillQueryCommands extends GrillCliApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestGrillQueryCommands.class);

  private GrillClient client;

  private static String explainPlan = "TOK_QUERY\n" +
      "   TOK_FROM\n" +
      "      TOK_TABREF\n" +
      "         TOK_TABNAME\n" +
      "            local_dim_table\n" +
      "         test_dim\n" +
      "   TOK_INSERT\n" +
      "      TOK_DESTINATION\n" +
      "         TOK_DIR\n" +
      "            TOK_TMP_FILE\n" +
      "      TOK_SELECT\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               id\n" +
      "         TOK_SELEXPR\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               name\n" +
      "      TOK_WHERE\n" +
      "         =\n" +
      "            .\n" +
      "               TOK_TABLE_OR_COL\n" +
      "                  test_dim\n" +
      "               dt\n" +
      "            'latest'";

  @Test
  public void testQueryCommands() throws Exception {
    client = new GrillClient();
    client.setConnectionParam("grill.persistent.resultset.indriver", "false");
    setup(client);
    GrillQueryCommands qCom = new GrillQueryCommands();
    qCom.setClient(client);
    testStoreResultSetInFile(qCom);
    testExecuteSyncQuery(qCom);
    testExecuteAsyncQuery(qCom);
    testExplainQuery(qCom);
    testPreparedQuery(qCom);
    testShowPersistentResultSet(qCom);
  }

  private void testStoreResultSetInFile(GrillQueryCommands qCom) {
    File file = new File("/tmp");
    Assert.assertTrue(
        qCom.storeResultSetInFile("1\tfirst", file.getAbsolutePath()).contains(
            "Entered path is a directory, please enter file name."));

    file = new File("/temporary/tempResult.txt");
    Assert.assertTrue(
        qCom.storeResultSetInFile("1\tfirst", file.getAbsolutePath()).contains(
            "Parent directory doesn't exist, please check the path."));

    file = new File("/tmp/tempResult.txt");
    Assert.assertTrue(
        qCom.storeResultSetInFile("1\tfirst", file.getAbsolutePath()).contains(
            "Results saved to: " + file.getAbsolutePath()));
  }

  private void testPreparedQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from test_dim";
    String result = qCom.getAllPreparedQueries();

    Assert.assertEquals("No prepared queries", result);
    String qh = qCom.prepare(sql);
    result = qCom.getAllPreparedQueries();
    Assert.assertEquals(qh, result);

    result = qCom.getPreparedStatus(qh);
    Assert.assertTrue(result.contains("User query:cube select id, name from test_dim"));
    Assert.assertTrue(result.contains(qh));

    //Test sync prepared query without output location
    result = qCom.executePreparedQuery(qh, false, null);

    LOG.warn("XXXXXX Prepared query sync result is  " + result);
    Assert.assertTrue(result.contains("1\tfirst"));

    //Test sync prepared query with output location
    try {
      File file = File.createTempFile("result","tmp");
      String message = qCom.executePreparedQuery(qh, false,
          file.getAbsolutePath());
      Assert.assertTrue(message.contains("Results saved to: " + file.getAbsolutePath()));

      FileReader fileReader = new FileReader(file);
      BufferedReader br = new BufferedReader(fileReader);
      StringBuilder resultFromFile = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        resultFromFile.append (line);
        resultFromFile.append ("\n");
      }
      Assert.assertTrue(resultFromFile.toString().contains("1\tfirst"));
      closeReaders(fileReader, br);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Failed during file IO operations while testing prepared sync query execution: " + e.getMessage());
    }

    //Test Async prepared query without output location
    String handle = qCom.executePreparedQuery(qh, true, null);
    LOG.debug("Perpared query handle is   " + handle);
    while(!client.getQueryStatus(handle).isFinished()) {
      Thread.sleep(5000);
    }
    String status = qCom.getStatus(handle);
    LOG.debug("Prepared Query Status is  " + status);
    Assert.assertTrue(status.contains("Status : SUCCESSFUL"));

    result = qCom.getQueryResults(handle, null);
    LOG.debug("Prepared Query Result is  " + result);
    Assert.assertTrue(result.contains("1\tfirst"));

    //Test Async prepared query with output location
    try {
      File file = File.createTempFile("result","tmp");
      handle = qCom.executePreparedQuery(qh, true, file.getAbsolutePath());

      while(!client.getQueryStatus(handle).isFinished()) {
        Thread.sleep(5000);
      }

      result = qCom.getQueryResults(handle, file.getAbsolutePath());
      Assert.assertTrue(result.contains("Results saved to: " + file.getAbsolutePath()));

      FileReader fileReader = new FileReader(file);
      BufferedReader br = new BufferedReader(fileReader);
      StringBuilder resultFromFile = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        resultFromFile.append (line);
        resultFromFile.append ("\n");
      }
      Assert.assertTrue(resultFromFile.toString().contains("1\tfirst"));
      closeReaders(fileReader, br);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Failed during file IO operations while testing async query execution: " + e.getMessage());
    }

    result = qCom.destroyPreparedQuery(qh);

    LOG.debug("destroy result is " + result);
    Assert.assertEquals("Successfully destroyed " + qh, result);

    result = qCom.explainAndPrepare(sql);
    Assert.assertTrue(result.contains(explainPlan));
    qh = qCom.getAllPreparedQueries();
    Assert.assertTrue(result.contains("Prepare handle:"+ qh));
    result = qCom.destroyPreparedQuery(qh);
    Assert.assertEquals("Successfully destroyed " + qh, result);

  }

  private void testExplainQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id, name from test_dim";
    String result = qCom.explainQuery(sql, "");

    LOG.debug(result);
    Assert.assertTrue(result.contains(explainPlan));

  }

  private void testExecuteAsyncQuery(GrillQueryCommands qCom) throws Exception {
    String sql = "cube select id,name from test_dim";
    String qh = qCom.executeQuery(sql, true, null);
    String result = qCom.getAllQueries("","");
    //this is because previous query has run two query handle will be there
    Assert.assertTrue(result.contains(qh));

    while(!client.getQueryStatus(qh).isFinished()) {
      Thread.sleep(5000);
    }

    Assert.assertTrue(qCom.getStatus(qh).contains("Status : SUCCESSFUL"));

    result = qCom.getQueryResults(qh, null);
    Assert.assertTrue(result.contains("1\tfirst"));
    //Kill query is not tested as there is no deterministic way of killing a query

    try {
      File file = File.createTempFile("result","tmp");
      String handle = qCom.executeQuery(sql, true, file.getAbsolutePath());

      while(!client.getQueryStatus(handle).isFinished()) {
        Thread.sleep(5000);
      }

      result = qCom.getQueryResults(handle, file.getAbsolutePath());
      Assert.assertTrue(result.contains("Results saved to: " + file.getAbsolutePath()));

      FileReader fileReader = new FileReader(file);
      BufferedReader br = new BufferedReader(fileReader);
      StringBuilder resultFromFile = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        resultFromFile.append (line);
        resultFromFile.append ("\n");
      }
      Assert.assertTrue(resultFromFile.toString().contains("1\tfirst"));
      closeReaders(fileReader, br);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Failed during file IO operations while testing async query execution: " + e.getMessage());
    }

    result = qCom.getAllQueries("SUCCESSFUL","");
    Assert.assertTrue(result.contains(qh));

    result = qCom.getAllQueries("FAILED","");
    Assert.assertTrue(result.contains("No queries"));

    String user = client.getGrillStatement(new QueryHandle(UUID.fromString(qh))).getQuery().getSubmittedUser();
    result = qCom.getAllQueries("",user);
    Assert.assertTrue(result.contains(qh));

    result = qCom.getAllQueries("","dummyuser");
    Assert.assertTrue(result.contains("No queries"));

    result = qCom.getAllQueries("SUCCESSFUL",user);
    Assert.assertTrue(result.contains(qh));
  }


  public void setup(GrillClient client) throws Exception {
    GrillCubeCommands command = new GrillCubeCommands();
    command.setClient(client);

    LOG.debug("Starting to test cube commands");
    URL cubeSpec =
        TestGrillQueryCommands.class.getClassLoader().getResource("sample-cube.xml");
    command.createCube(new File(cubeSpec.toURI()).getAbsolutePath());
    TestGrillDimensionCommands.createDimension();
    TestGrillDimensionTableCommands.addDim1Table("dim_table",
        "dim_table.xml", "dim_table_storage.xml", "local");

    URL dataFile =
        TestGrillQueryCommands.class.getClassLoader().getResource("data.txt");

    QueryHandle qh = client.executeQueryAsynch("LOAD DATA LOCAL INPATH '"
        + new File(dataFile.toURI()).getAbsolutePath()+
        "' OVERWRITE INTO TABLE local_dim_table partition(dt='latest')");

    while(!client.getQueryStatus(qh).isFinished()) {
      Thread.sleep(5000);
    }

    Assert.assertEquals(client.getQueryStatus(qh).getStatus(),QueryStatus.Status.SUCCESSFUL);
  }

  private void testExecuteSyncQuery(GrillQueryCommands qCom) {
    String sql = "cube select id,name from test_dim";
    String result = qCom.executeQuery(sql, false, null);
    Assert.assertTrue(result.contains("1\tfirst"), result);

    try {
      File file = File.createTempFile("result","tmp");
      String message = qCom.executeQuery(sql, false, file.getAbsolutePath());
      Assert.assertTrue(message.contains("Results saved to: " + file.getAbsolutePath()));

      FileReader fileReader = new FileReader(file);
      BufferedReader br = new BufferedReader(fileReader);
      StringBuilder resultFromFile = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        resultFromFile.append (line);
        resultFromFile.append ("\n");
      }
      Assert.assertTrue(resultFromFile.toString().contains("1\tfirst"));
      closeReaders(fileReader, br);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Failed during file IO operations while testing sync query execution: " + e.getMessage());
    }

  }

  private void testShowPersistentResultSet(GrillQueryCommands qCom) throws Exception {
    System.out.println("@@PERSISTENT_RESULT_TEST-------------");
    client.setConnectionParam("grill.persistent.resultset.indriver", "true");
    String query = "cube select id,name from test_dim";
    try {
      String result = qCom.executeQuery(query, false, null);
      System.out.println("@@ RESULT " + result);
      Assert.assertNotNull(result);
      Assert.assertFalse(result.contains("Failed to get resultset"));
    } catch (Exception exc) {
      exc.printStackTrace();
      Assert.fail("Exception not expected: " + exc.getMessage());
    }
    System.out.println("@@END_PERSISTENT_RESULT_TEST-------------");
  }

  private void closeReaders(FileReader fileReader, BufferedReader br)
      throws IOException {
    if (br != null) {
      br.close();
    }
    if (fileReader != null) {
      fileReader.close();
    }
  }

}
