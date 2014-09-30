package com.inmobi.grill.server;

import org.apache.hadoop.hive.conf.HiveConf;

public class GrillServerConf {

  public static volatile HiveConf conf;

  public static HiveConf get() {
    if(conf == null) {
      synchronized (GrillServerConf.class) {
        if (conf == null) {
          conf = new HiveConf();
          conf.addResource("grillserver-default.xml");
          conf.addResource("grill-site.xml");
        }
      }
    }
    return conf;
  }
}
