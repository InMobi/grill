package com.inmobi.grill.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class TestReport {
  @XmlElement @Getter private String testTable;
  @XmlElement @Getter private String outputTable;
  @XmlElement @Getter private String outputColumn;
  @XmlElement @Getter private String labelColumn;
  @XmlElement @Getter private String featureColumns;
  @XmlElement @Getter private String algorithm;
  @XmlElement @Getter private String modelID;
  @XmlElement @Getter private String reportID;
  @XmlElement @Getter private String queryID;
}
