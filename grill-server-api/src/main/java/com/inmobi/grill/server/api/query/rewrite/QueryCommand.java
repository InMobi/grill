package com.inmobi.grill.server.api.query.rewrite;

/*
 * #%L
 * Grill API for server and extensions
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

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Query Command which is passed to Query rewriters
 */
public abstract class QueryCommand {

  protected QueryCommand(QueryCommand queryCommand) {
    this.command = queryCommand.getCommand();
    this.userName = queryCommand.getUserName();
    this.conf = queryCommand.getConf();
  }

  /**
   * Type  of query to be rewritten
   */
  public static enum Type {
    HQL("HQL", "Hive Query language", null),
    NONSQL("NonSQL", "Non SQL commands like add/set", HQL),
    CUBE("CUBEQL", "CubeQL", HQL),
    DOMAIN("DSL", "Domain specific language", CUBE, HQL, NONSQL);

    private final String name;
    private final String description;
    private Set<Type> nextValidStates;

    private Type(String name, String description, Type... hql) {
      this.name=name;
      this.description=description;
      this.nextValidStates = new HashSet<Type>();
      nextValidStates.addAll(Arrays.asList(hql));
    }

    public String getName() {
      return this.name;
    }

    public String getDescription() {
      return description;
    }

    public Set<Type> getNextValidStates() {
       return nextValidStates;
    }

  }

  public abstract Type getType();

  /**
   * The command submitted
   */
  @Getter @Setter
  protected String command;

  /**
   * User who submitted the query
   */

  @Getter @Setter
  protected String userName;

  /**
   * Configuration associated with the query/command
   */
  @Getter @Setter
  protected Configuration conf;

  public QueryCommand() {
    super();
  }

  protected QueryCommand(String command, String submittedUser, Configuration conf) {
    this.command = command;
    this.userName = submittedUser;
    this.conf = conf;
  }

  /**
   * Matches passed command with the expected query pattern
   * @param line
   * @return true if matches
   * @return false if not
   */
  public abstract boolean matches(String line);

  /**
   * Rewrite the given command to another type
   *
   * @return the rewritten query command
   * @throws RewriteException if there is an error during rewrite
   */
  public abstract QueryCommand rewrite() throws RewriteException;

}
