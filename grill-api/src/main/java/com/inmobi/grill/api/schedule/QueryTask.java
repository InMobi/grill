package com.inmobi.grill.api.schedule;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import com.inmobi.grill.api.GrillConf;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryTask extends ScheduleTask {
  @XmlElement @Getter private String query;
  @XmlElement @Getter private GrillConf conf;
  @XmlElement @Getter private String jarPaths;
}
