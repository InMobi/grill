/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata;

<<<<<<< HEAD
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.lens.cube.metadata.SchemaGraph.JoinPath;
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;
=======
import java.util.*;

import org.apache.lens.cube.metadata.SchemaGraph.JoinPath;
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

@EqualsAndHashCode
@ToString
public class JoinChain implements Named {
<<<<<<< HEAD
  @Getter private final String name;
  @Getter private final String displayString;
  @Getter private final String description;
  // There can be more than one path associated with same name.
  // c1.r1->t1.k1
  // c1.r2->t1.k2
  @Getter private final List<Path> paths;


  public void addProperties(AbstractCubeTable tbl) {
    if(tbl instanceof Cube) {
      addProperties((Cube)tbl);
=======
  @Getter
  private final String name;
  @Getter
  private final String displayString;
  @Getter
  private final String description;
  // There can be more than one path associated with same name.
  // c1.r1->t1.k1
  // c1.r2->t1.k2
  @Getter
  private final List<Path> paths;


  public void addProperties(AbstractCubeTable tbl) {
    if (tbl instanceof Cube) {
      addProperties((Cube) tbl);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    } else {
      addProperties((Dimension) tbl);
    }
  }

  public void addProperties(Cube cube) {
    Map<String, String> props = cube.getProperties();
    props.put(MetastoreUtil.getCubeJoinChainNumChainsKey(getName()), String.valueOf(paths.size()));
<<<<<<< HEAD
    for (int i = 0; i< paths.size(); i++) {
      props.put(MetastoreUtil.getCubeJoinChainFullChainKey(getName(), i),
          MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
=======
    for (int i = 0; i < paths.size(); i++) {
      props.put(MetastoreUtil.getCubeJoinChainFullChainKey(getName(), i),
        MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    }
    props.put(MetastoreUtil.getCubeJoinChainDisplayKey(getName()), displayString);
    props.put(MetastoreUtil.getCubeJoinChainDescriptionKey(getName()), description);
  }

  public void addProperties(Dimension dimension) {
    Map<String, String> props = dimension.getProperties();
    props.put(MetastoreUtil.getDimensionJoinChainNumChainsKey(getName()), String.valueOf(paths.size()));
<<<<<<< HEAD
    for (int i = 0; i< paths.size(); i++) {
      props.put(MetastoreUtil.getDimensionJoinChainFullChainKey(getName(), i),
                MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
=======
    for (int i = 0; i < paths.size(); i++) {
      props.put(MetastoreUtil.getDimensionJoinChainFullChainKey(getName(), i),
        MetastoreUtil.getReferencesString(paths.get(i).getReferences()));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    }
    props.put(MetastoreUtil.getDimensionJoinChainDisplayKey(getName()), displayString);
    props.put(MetastoreUtil.getDimensionJoinChainDescriptionKey(getName()), description);
  }


  /**
   * Construct join chain
   *
   * @param name
   * @param display
   * @param description
   */
  public JoinChain(String name, String display, String description) {
    this.name = name.toLowerCase();
    this.displayString = display;
    this.description = description;
    this.paths = new ArrayList<Path>();
  }

  /**
   * This is used only for serializing
   *
   * @param table
   * @param name
   */
  public JoinChain(AbstractCubeTable table, String name) {
    boolean isCube = (table instanceof Cube);
    this.name = name;
    this.paths = new ArrayList<Path>();
    int numChains = 0;

    Map<String, String> props = table.getProperties();
<<<<<<< HEAD
    if(isCube) {
=======
    if (isCube) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      numChains = Integer.parseInt(props.get(MetastoreUtil.getCubeJoinChainNumChainsKey(getName())));
    } else {
      numChains = Integer.parseInt(props.get(MetastoreUtil.getDimensionJoinChainNumChainsKey(getName())));
    }
    for (int i = 0; i < numChains; i++) {
      Path chain = new Path();
      String refListStr;
<<<<<<< HEAD
      if(isCube) {
=======
      if (isCube) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
        refListStr = props.get(MetastoreUtil.getCubeJoinChainFullChainKey(getName(), i));
      } else {
        refListStr = props.get(MetastoreUtil.getDimensionJoinChainFullChainKey(getName(), i));
      }
<<<<<<< HEAD
      String refListDims[] = StringUtils.split(refListStr, ",");
=======
      String[] refListDims = StringUtils.split(refListStr, ",");
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      TableReference from = null;
      for (String refDimRaw : refListDims) {
        if (from == null) {
          from = new TableReference(refDimRaw);
        } else {
          chain.addLink(new Edge(from, new TableReference(refDimRaw)));
          from = null;
        }
      }
      paths.add(chain);
    }
<<<<<<< HEAD
    if(isCube) {
=======
    if (isCube) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      this.description = props.get(MetastoreUtil.getCubeJoinChainDescriptionKey(name));
      this.displayString = props.get(MetastoreUtil.getCubeJoinChainDisplayKey(name));
    } else {
      this.description = props.get(MetastoreUtil.getDimensionJoinChainDescriptionKey(name));
      this.displayString = props.get(MetastoreUtil.getDimensionJoinChainDisplayKey(name));
    }

  }

  /**
   * Copy constructor for JoinChain
   *
   * @param other JoinChain
   */
  public JoinChain(JoinChain other) {
    this.name = other.name;
    this.displayString = other.displayString;
    this.description = other.description;
    this.paths = new ArrayList<Path>(other.paths);
  }

<<<<<<< HEAD
  @EqualsAndHashCode(exclude={"relationShip"})
  @ToString
  private static class Edge {
    final TableReference from;
    final TableReference to;
    transient TableRelationship relationShip = null;
=======
  @EqualsAndHashCode(exclude = {"relationShip"})
  @ToString
  public static class Edge {
    @Getter
    private final TableReference from;
    @Getter
    private final TableReference to;
    transient TableRelationship relationShip = null;

>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    Edge(TableReference from, TableReference to) {
      this.from = from;
      this.to = to;
    }

    TableRelationship toDimToDimRelationship(CubeMetastoreClient client) throws HiveException {
      if (relationShip == null) {
        relationShip = new TableRelationship(from.getDestColumn(),
<<<<<<< HEAD
        client.getDimension(from.getDestTable()),
        to.getDestColumn(),
        client.getDimension(to.getDestTable()));
=======
          client.getDimension(from.getDestTable()),
          to.getDestColumn(),
          client.getDimension(to.getDestTable()));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      }
      return relationShip;
    }

    /**
     * return Cube or Dimension relationship depending on the source table of the join chain.
<<<<<<< HEAD
=======
     *
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
     * @param client
     * @return
     * @throws HiveException
     */
    TableRelationship toCubeOrDimRelationship(CubeMetastoreClient client) throws HiveException {
      if (relationShip == null) {
        AbstractCubeTable fromTable = null;
<<<<<<< HEAD
        if(client.isCube(from.getDestTable())) {
          fromTable = (AbstractCubeTable)client.getCube(from.getDestTable());
        } else if(client.isDimension(from.getDestTable())) {
          fromTable = client.getDimension(from.getDestTable());
        }

        if(fromTable != null) {
          relationShip = new TableRelationship(from.getDestColumn(),
                                               fromTable,
                                               to.getDestColumn(),
                                               client.getDimension(to.getDestTable()));
=======
        if (client.isCube(from.getDestTable())) {
          fromTable = (AbstractCubeTable) client.getCube(from.getDestTable());
        } else if (client.isDimension(from.getDestTable())) {
          fromTable = client.getDimension(from.getDestTable());
        }

        if (fromTable != null) {
          relationShip = new TableRelationship(from.getDestColumn(),
            fromTable,
            to.getDestColumn(),
            client.getDimension(to.getDestTable()));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
        }
      }
      return relationShip;
    }

  }

<<<<<<< HEAD
  @EqualsAndHashCode(exclude={"refs"})
  @ToString
  public static class Path {
=======
  @EqualsAndHashCode(exclude = {"refs"})
  @ToString
  public static class Path {
    @Getter
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    final List<Edge> links = new ArrayList<Edge>();
    transient List<TableReference> refs = null;

    private void addLink(Edge edge) {
      links.add(edge);
    }

    public List<TableReference> getReferences() {
      if (refs == null) {
        refs = new ArrayList<TableReference>();
        for (Edge edge : links) {
          refs.add(edge.from);
          refs.add(edge.to);
        }
      }
      return refs;
    }

    Path() {
    }

<<<<<<< HEAD
    Path (List<TableReference> refs) {
      for (int i = 0; i < refs.size() - 1; i += 2) {
        addLink(new Edge(refs.get(i), refs.get(i+1)));
=======
    Path(List<TableReference> refs) {
      for (int i = 0; i < refs.size() - 1; i += 2) {
        addLink(new Edge(refs.get(i), refs.get(i + 1)));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      }
    }

    String getDestTable() {
<<<<<<< HEAD
      return links.get(links.size() -1).to.getDestTable();
=======
      return links.get(links.size() - 1).to.getDestTable();
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    }

    String getSrcColumn() {
      return links.get(0).from.getDestColumn();
    }
  }

  public void addPath(List<TableReference> refs) {
<<<<<<< HEAD
    if (refs.size() <= 1 || refs.size() %2 != 0) {
=======
    if (refs.size() <= 1 || refs.size() % 2 != 0) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      throw new IllegalArgumentException("Path should contain both from and to links");
    }
    this.paths.add(new Path(refs));
  }

  private transient String destTable = null;
<<<<<<< HEAD
=======

>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
  /**
   * Get final destination table
   *
   * @return
   */
  public String getDestTable() {
    if (destTable == null) {
<<<<<<< HEAD
    for (Path path : paths) {
      if (destTable == null) {
        destTable = path.getDestTable();
      } else {
        String temp = path.getDestTable();
        if (!destTable.equalsIgnoreCase(temp)) {
          throw new IllegalArgumentException("Paths have different destination tables :" + destTable + "," + temp);
        }
      }
    }
    }
=======
      for (Path path : paths) {
        if (destTable == null) {
          destTable = path.getDestTable();
        } else {
          String temp = path.getDestTable();
          if (!destTable.equalsIgnoreCase(temp)) {
            throw new IllegalArgumentException("Paths have different destination tables :" + destTable + "," + temp);
          }
        }
      }
    }
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    return destTable;
  }

  /**
   * Get all source columns from cube
   *
   * @return
   */
  public Set<String> getSourceColumns() {
    Set<String> srcFields = new HashSet<String>();
    for (Path path : paths) {
      srcFields.add(path.getSrcColumn());
    }
    return srcFields;
  }

  /**
   * Get all dimensions involved in paths, except destination table
   *
   * @return
   */
  public Set<String> getIntermediateDimensions() {
    Set<String> dimNames = new HashSet<String>();
    for (Path path : paths) {
      // skip last entry in all paths, as that would be a destination
      for (int i = 0; i < path.links.size() - 1; i++) {
        dimNames.add(path.links.get(i).to.getDestTable());
      }
    }
    return dimNames;
  }

  /**
   * Convert join paths to schemaGraph's JoinPath
   *
   * @param client
<<<<<<< HEAD
   *
   * @return List<SchemaGraph.JoinPath>
   *
=======
   * @return List<SchemaGraph.JoinPath>
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   * @throws HiveException
   */
  public List<SchemaGraph.JoinPath> getRelationEdges(CubeMetastoreClient client) throws HiveException {
    List<SchemaGraph.JoinPath> schemaGraphPaths = new ArrayList<SchemaGraph.JoinPath>();
    for (Path path : paths) {
      JoinPath jp = new JoinPath();
      // Add edges from dimension to cube
<<<<<<< HEAD
      for (int i = path.links.size() -1; i> 0; i -= 1) {
=======
      for (int i = path.links.size() - 1; i > 0; i -= 1) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
        jp.addEdge(path.links.get(i).toDimToDimRelationship(client));
      }
      jp.addEdge(path.links.get(0).toCubeOrDimRelationship(client));
      schemaGraphPaths.add(jp);
    }
    return schemaGraphPaths;
  }
}
