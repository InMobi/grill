package com.inmobi.grill.ml.workflow;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.concurrent.TimeUnit;

@XmlRootElement
@AllArgsConstructor
public class WorkflowFrequency {
  @Getter private final int intervals;
  @Getter private final TimeUnit timeUnit;
}
