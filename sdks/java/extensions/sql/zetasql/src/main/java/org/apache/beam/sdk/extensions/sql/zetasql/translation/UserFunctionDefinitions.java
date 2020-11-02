/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.AggregateFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Holds user defined function definitions. */
public class UserFunctionDefinitions {
  public final ImmutableMap<List<String>, ResolvedNodes.ResolvedCreateFunctionStmt>
      sqlScalarFunctions;

  /**
   * SQL native user-defined table-valued function can be resolved by Analyzer. Keeping the function
   * name to its ResolvedNode mapping so during Plan conversion, UDTVF implementation can replace
   * inputs of TVFScanConverter.
   */
  public final ImmutableMap<List<String>, ResolvedNode> sqlTableValuedFunctions;

  public final ImmutableMap<List<String>, Method> javaScalarFunctions;
  public final ImmutableMap<List<String>, AggregateFn> javaAggregateFunctions;

  public UserFunctionDefinitions(
      ImmutableMap<List<String>, ResolvedNodes.ResolvedCreateFunctionStmt> sqlScalarFunctions,
      ImmutableMap<List<String>, ResolvedNode> sqlTableValuedFunctions,
      ImmutableMap<List<String>, Method> javaScalarFunctions,
      ImmutableMap<List<String>, AggregateFn> javaAggregateFunctions) {
    this.sqlScalarFunctions = sqlScalarFunctions;
    this.sqlTableValuedFunctions = sqlTableValuedFunctions;
    this.javaScalarFunctions = javaScalarFunctions;
    this.javaAggregateFunctions = javaAggregateFunctions;
  }
}
