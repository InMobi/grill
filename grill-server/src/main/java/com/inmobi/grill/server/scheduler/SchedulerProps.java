package com.inmobi.grill.server.scheduler;

import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.inmobi.grill.api.GrillException;

/**
 * Class to populate the scheduler related properties (db: url, user, pass)
 * 
 * @author prashant.gupta
 * 
 */
public class SchedulerProps {
  private static final Logger LOGGER = Logger.getLogger(SchedulerProps.class
      .getName());
  private static final String SCHEDULER_PROPS_FILE = "scheduler.properties";

  public SchedulerProps() {
    super();
  }

  public static enum PropNames {
    SCHEDULER_DB_URL, SCHEDULER_DB_USER, SCHEDULER_DB_PASS,
  }

  private static Hashtable<String, String> propMap =
      new Hashtable<String, String>();

  static {
    try {
      final Properties p = new Properties();

      p.load(Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(SCHEDULER_PROPS_FILE));
      loadProps(p);
    } catch (final GrillException e) {
      throw new RuntimeException("Error while loading properties", e);
    } catch (final Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(
          "Error while initializing SchedulerPluginProps", e);
    }
  }

  private static void loadProps(final Properties p) throws GrillException {
    try {
      propMap.put(PropNames.SCHEDULER_DB_URL.name(), p.get("scheduler.db.url")
          .toString());
      propMap.put(PropNames.SCHEDULER_DB_USER.name(), p
          .get("scheduler.db.user").toString());
      propMap.put(PropNames.SCHEDULER_DB_PASS.name(), p
          .get("scheduler.db.pass").toString());
    } catch (final Exception e) {
      throw new GrillException("Error while loading system props: "
          + e.getMessage());
    }
  }

  public static String get(PropNames name) {
    return propMap.get(name.name());
  }
}
