package com.inmobi.grill.api.query;


import lombok.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NamedQueryList {
  @Getter
  @Setter
  private List<NamedQuery> elements;
}