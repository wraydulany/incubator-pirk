/*
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
package org.apache.pirk.schema.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pirk.schema.query.filter.DataFilter;

/**
 * Class to hold a query schema
 *
 */
public class QuerySchema implements Serializable
{
  private static final long serialVersionUID = 1L;

  // This schema's name.
  private final String schemaName;

  // Name of the data schema associated with this query schema.
  private final String dataSchemaName;

  // Name of element in the dataSchema to be used as the selector.
  private final String selectorName;

  // Element names from the data schema to include in the response.
  // Order matters for packing/unpacking.
  private final List<String> elementNames = new ArrayList<String>();

  // Name of class to use in data filtering.
  private final String filterTypeName;

  // Instance of the filterTypeName.
  private final DataFilter filter;

  // Set of data schema element names on which to apply filtering.
  private final Set<String> filteredElementNames = new HashSet<>();

  // Total number of bits to be returned for each data element hit.
  private final int dataElementSize;

  QuerySchema(String schemaName, String dataSchemaName, String selectorName, String filterTypeName, DataFilter filter, int dataElementSize)
  {
    this.schemaName = schemaName;
    this.dataSchemaName = dataSchemaName;
    this.selectorName = selectorName;
    this.filterTypeName = filterTypeName;
    this.filter = filter;
    this.dataElementSize = dataElementSize;
  }

  /**
   * Returns the name of this schema.
   * 
   * @return The schema name.
   */
  public String getSchemaName()
  {
    return schemaName;
  }

  /**
   * Returns the name of the data schema.
   * <p>
   * This query is designed to be run over data described by this data schema.
   *
   * @return The data schema name.
   */
  public String getDataSchemaName()
  {
    return dataSchemaName;
  }

  /**
   * Returns the element names to include in the response.
   * <p>
   * The element names are defined by the data schema associated with this query.
   * 
   * @return The ordered list of query element names.
   */
  public List<String> getElementNames()
  {
    return elementNames;
  }

  /**
   * Returns the element name used as the selector.
   * <p>
   * The element names are defined by the data schema associated with this query.
   * 
   * @return The element names being selected.
   */
  public String getSelectorName()
  {
    return selectorName;
  }

  public int getDataElementSize()
  {
    return dataElementSize;
  }

  /**
   * Returns the name of the filter class for this query.
   * 
   * The filter class name is the fully qualified name of a Java class that implements the {@link DataFilter} interface.
   * 
   * @return The type name of the query filter, or <code>null</code> if there is no filter defined.
   */
  public String getFilterTypeName()
  {
    return filterTypeName;
  }

  /**
   * Returns the set of element names on which to apply the filter.
   * 
   * @return The possibly empty set of data schema element names.
   */
  public Set<String> getFilteredElementNames()
  {
    return filteredElementNames;
  }

  /**
   * Returns the data element filter for this query.
   * <p>
   * The data filter is applied to the {@link QuerySchema#getFilteredElementNames()} data elements.
   * 
   * @return The data filter, or <code>null</code> if no filter has been specified for this query.
   */
  public DataFilter getFilter()
  {
    return filter;
  }
}
