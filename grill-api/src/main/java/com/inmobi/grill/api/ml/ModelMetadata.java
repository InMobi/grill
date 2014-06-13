package com.inmobi.grill.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class ModelMetadata {
  @XmlElement @Getter
  private String modelID;

  @XmlElement @Getter
  private String table;

  @XmlElement @Getter
  private String algorithm;

  @XmlElement @Getter
  private String params;

  @XmlElement @Getter
  private String createdAt;

  @XmlElement @Getter
  private String modelPath;
}
