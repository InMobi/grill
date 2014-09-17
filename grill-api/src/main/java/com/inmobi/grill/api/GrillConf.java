package com.inmobi.grill.api;

/*
 * #%L
 * Grill API
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.Getter;
import lombok.NoArgsConstructor;
import sun.misc.IOUtils;

@XmlRootElement(name = "conf")
@NoArgsConstructor
public class GrillConf implements Serializable {
  private static final long serialVersionUID = 1L;

  @XmlElementWrapper @Getter
  private final Map<String, String> properties = new HashMap<String, String>();

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public static GrillConf fromString(String confStr) throws Exception {
    ByteArrayInputStream xmlInputStream = null;

    try {
      xmlInputStream = new ByteArrayInputStream(confStr.getBytes());
      JAXBContext jaxbContext = JAXBContext.newInstance(GrillConf.class);
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      return (GrillConf) unmarshaller.unmarshal(xmlInputStream);
    } finally {
      if (xmlInputStream != null) {
        xmlInputStream.close();
      }
    }
  }

  public static String toXMLString(GrillConf conf) throws Exception {
    JAXBContext jaxbContext = JAXBContext.newInstance(GrillConf.class);
    Marshaller marshaller = jaxbContext.createMarshaller();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    marshaller.marshal(conf, out);
    return out.toString();
  }
}
