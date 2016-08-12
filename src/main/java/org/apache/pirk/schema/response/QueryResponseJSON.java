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
package org.apache.pirk.schema.response;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hadoop.io.Text;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;

import org.apache.pirk.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON helper class for query results
 * <p>
 * 
 */
public class QueryResponseJSON implements Serializable
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(QueryResponseJSON.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private JsonNode jsonNode = null;

  private DataSchema dSchema = null;

  private QueryInfo queryInfo = null;

  public static final String EVENT_TYPE = "event_type"; // notification type the matched the record
  public static final Text EVENT_TYPE_TEXT = new Text(EVENT_TYPE);

  public static final String QUERY_ID = "query_id"; // query ID that generated the notification
  public static final Text QUERY_ID_TEXT = new Text(QUERY_ID);

  public static final String SELECTOR = "match"; // tag for selector that generated the hit
  public static final Text SELECTOR_TEXT = new Text(SELECTOR);

  /**
   * Constructor with data schema checking
   */
  public QueryResponseJSON(QueryInfo queryInfoIn)
  {
    queryInfo = queryInfoIn;

    if (queryInfo == null)
    {
      logger.info("queryInfo is null");
    }

    QuerySchema qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    jsonNode = mapper.createObjectNode();
    setGeneralQueryResponseFields(queryInfo);
  }

  /**
   * Constructor with no data schema checking
   */
  public QueryResponseJSON()
  {
    jsonNode = mapper.createObjectNode();
  }

  /**
   * Constructor with no data schema checking
   */
  public QueryResponseJSON(String jsonString)
  {
    try
    {
      jsonNode = (ObjectNode) mapper.readTree(jsonString);
    } catch (IOException e)
    {
      logger.error("Unable to parse json string.");
      e.printStackTrace();
    }
  }

  public JsonNode getJsonNode()
  {
    return jsonNode;
  }

  public String getJSONString()
  {
    return jsonNode.toString();
  }

  public Object getValue(String key)
  {
    return StringUtils.jacksonSimpleTypeHelper(jsonNode.get(key));
  }

  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  // Create empty JSON object based on the DataSchema
  @SuppressWarnings("unchecked")
  private void initialize()
  {
    Set<String> schemaStringRep = dSchema.getNonArrayElements();
    for (String key : schemaStringRep)
    {
      ((ObjectNode)jsonNode).put(key, "");
    }
    Set<String> schemaListRep = dSchema.getArrayElements();
    for (String key : schemaListRep)
    {
      ((ObjectNode)jsonNode).putArray(key);
    }
  }

  /**
   * Add a <key,value> pair to the response object; checks the data schema if this QueryResponseJSON object was instantiated with schema checking (with a
   * QueryInfo object)
   */
  @SuppressWarnings("unchecked")
  public void setMapping(String key, Object val)
  {
    if (dSchema == null)
    {
      StringUtils.jacksonSimpleTypePutterHelper(jsonNode, key, val);
    }
    else
    {
      if (dSchema.getArrayElements().contains(key))
      {
        // If val is not an instance of ArrayList, we pretend it doesn't exist and make an empty record for this array.
        // No, what we should do if val is not itself an array list is just add it on to the array.
        if (!(val instanceof ArrayList))
        {
          ArrayList<Object> list = new ArrayList<>();
          // if the json node doesn't exist, make it.
          if (!jsonNode.has(key))
          {
            ((ObjectNode) jsonNode).putArray(key);
          }

          // TODO this needs to be done as an ArrayList<Object>.
          ArrayList<String> templist = StringUtils.jsonNodeArrayToArrayList(jsonNode.get(key));
          list.addAll(templist);


          if (!list.contains(val))
          {
            list.add(val);
          }
          for(Object element: list)
          {
            StringUtils.jacksonSimpleTypePutterHelper((ArrayNode) jsonNode.get(key), element);
          }
        }
        else
        {
          if (!jsonNode.has(key))
          {
            ((ObjectNode) jsonNode).putArray(key);
          }
          for(Object element: (ArrayList<Object>) val)
          {
            StringUtils.jacksonSimpleTypePutterHelper((ArrayNode) jsonNode.get(key), element);
          }
        }
      }
      else if (dSchema.getNonArrayElements().contains(key) || key.equals(SELECTOR))
      {
        StringUtils.jacksonSimpleTypePutterHelper(jsonNode, key, val);
      }
      else
      {
        logger.info("WARN: Schema does not contain key = " + key);
      }
    }
  }

  // Method to set the selector field explicitly
  @SuppressWarnings("unchecked")
  public void setSelector(Object val)
  {
    StringUtils.jacksonSimpleTypePutterHelper(jsonNode, SELECTOR, val);
  }

  public void setAllFields(HashMap<String,String> dataMap)
  {
    for (String key : dataMap.keySet())
    {
      setMapping(key, dataMap.get(key));
    }
  }

  /**
   * Method to set the common query response fields
   */
  @SuppressWarnings("unchecked")
  public void setGeneralQueryResponseFields(QueryInfo queryInfo)
  {
    ((ObjectNode) jsonNode).put(EVENT_TYPE, queryInfo.getQueryType());
    ((ObjectNode) jsonNode).put(QUERY_ID, queryInfo.getIdentifier().toString());
  }

  @Override
  public String toString()
  {
    return jsonNode.toString();
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jsonNode == null) ? 0 : jsonNode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryResponseJSON other = (QueryResponseJSON) obj;
    if (jsonNode == null)
    {
      if (other.jsonNode != null)
        return false;
    }
    else
    {
      Set<String> thisKeySet = StringUtils.jsonGetKeys(jsonNode);
      Set<String> otherKeySet = StringUtils.jsonGetKeys(other.jsonNode);

      if (!thisKeySet.equals(otherKeySet))
      {
        return false;
      }
      for (String key : thisKeySet)
      {
        if (!(jsonNode.get(key)).equals(other.jsonNode.get(key)))
        {
          return false;
        }
      }
    }
    return true;
  }
}
