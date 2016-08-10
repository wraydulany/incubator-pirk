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
package org.apache.pirk.utils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pirk.schema.data.DataSchema;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pirk-specific string utilities
 * 
 */
public class StringUtils
{
  private static final Logger logger = LoggerFactory.getLogger(StringUtils.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Method to convert a MapWritable into a JSON string
   * 
   */
  @SuppressWarnings("unchecked")
  public static String mapWritableToString(MapWritable map)
  {
    // Convert to JSON and then write to a String - ensures JSON read-in compatibility
    //ObjectMapper mapper = new ObjectMapper();
    String jsonString;
    ObjectNode rootnode = mapper.createObjectNode();
    for( Writable key : map.keySet()){
      rootnode.put(key.toString(), map.get(key).toString());
    }
    try
    {
      jsonString = mapper.writeValueAsString(rootnode);
    } catch (JsonProcessingException e)
    {
      logger.error("Error processing MapWritable into JSON.");
      e.printStackTrace();
      return map.toString();
    }

    return jsonString;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as JSON formatted String objects
   */
  public static MapWritable jsonStringToMapWritable(JsonNode jsonNode)
  {
    MapWritable mapWritable = new MapWritable();
    Map<String,Object> map = new HashMap<>();

    try
    {
      map = mapper.readValue(jsonNode.toString(), new TypeReference<Map<String,Object>>(){});
    } catch (IOException e)
    {
      logger.error("Unable to parse JSON string into Map<String, Object>: " + jsonNode.toString());
      e.printStackTrace();
    }
    Iterator it = map.entrySet().iterator();
    while(it.hasNext())
    {
      Map.Entry<String,Object> pair = (Map.Entry) it.next();
      if (jsonNode.get(pair.getKey()).isArray())
      {
        try
        {
          mapWritable.put(new Text(pair.getKey()), new Text(mapper.writeValueAsString(jsonNode.get(pair.getKey()))));
        } catch (JsonProcessingException e)
        {
          logger.error("Unable to parse previously parsed json string.");
          e.printStackTrace();
        }
      }
      else
      {
        mapWritable.put(new Text(pair.getKey()), new Text(pair.getValue().toString()));
      }

    }

    return mapWritable;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  public static MapWritable jsonStringToMapWritableWithWritableArrayWritable(JsonNode jsonNode, DataSchema dataSchema)
  {
    MapWritable mapWritable = new MapWritable();
    Map<String, Object> map = jsonStringToMap(jsonNode, dataSchema);

    Iterator it = map.entrySet().iterator();
    while(it.hasNext())
    {
      Map.Entry<String,Object> pair = (Map.Entry) it.next();
      Text mapKey = new Text(pair.getKey());
      if (dataSchema.isArrayElement(pair.getKey()))
      {
        WritableArrayWritable mapValue = StringUtils.jsonNodeArrayToWritableArrayWritable(jsonNode.get(pair.getKey()));
        mapWritable.put(mapKey, mapValue);
      }
      else
      {
        Text mapValue = new Text(pair.getValue().toString());
        mapWritable.put(mapKey, mapValue);
      }
    }

    return mapWritable;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  @SuppressWarnings("unchecked")
  public static MapWritable jsonStringToMapWritableWithArrayWritable(JsonNode jsonNode, DataSchema dataSchema)
  {
    MapWritable mapWritable = new MapWritable();
    Map<String, Object> map = jsonStringToMap(jsonNode, dataSchema);

    Iterator it = map.entrySet().iterator();
    while(it.hasNext())
    {
      Map.Entry<String,Object> pair = (Map.Entry) it.next();
      Text mapKey = new Text(pair.getKey());
      if (dataSchema.isArrayElement(pair.getKey()))
      {
        ArrayWritable mapValue = StringUtils.jsonNodeArrayToArrayWritable(jsonNode.get(pair.getKey()));
        mapWritable.put(mapKey, mapValue);
      }
      else
      {
        Text mapValue = new Text(pair.getValue().toString());
        mapWritable.put(mapKey, mapValue);
      }
    }

    return mapWritable;
  }

  /**
   * Method to take an input json string and output a Map<String, Object> with arrays as ArrayList<String> objects and single values as String objects
   *
   * Performs minor type validation against the dataSchema.
   */
  @SuppressWarnings("unchecked")
  public static Map<String,Object> jsonStringToMap(JsonNode jsonNode, DataSchema dataSchema)
  {
    Map<String,Object> map = new HashMap<>();

    try
    {
      map = mapper.readValue(jsonNode.toString(), new TypeReference<Map<String,Object>>(){});

      Iterator it = map.entrySet().iterator();
      while(it.hasNext())
      {
        Map.Entry<String,Object> pair = (Map.Entry) it.next();
        if (dataSchema.isArrayElement(pair.getKey()))// && !jsonNode.get(pair.getKey()).isArray())
        {
          List<String> temparray = ((ArrayList<Object>) pair.getValue()).stream().map(object -> Objects.toString(object,null)).collect(Collectors.toList());
          map.put(pair.getKey(), (ArrayList<String>) temparray);
        }
        else
        {
          map.put(pair.getKey(), pair.getValue().toString());
        }
      }
    } catch (IOException e)
    {
      logger.error("Unable to parse JSON string into Map<String, Object>: " + jsonNode.toString());
      e.printStackTrace();
    }

    return map;
  }

  /**
   * Method to take an input json array format string and output a WritableArrayWritable
   */
  public static WritableArrayWritable jsonNodeArrayToWritableArrayWritable(JsonNode jsonNode)
  {
    return new WritableArrayWritable(jsonNodeArrayToList(jsonNode));
  }

  /**
   * Method to take an input json array format string and output an ArrayWritable
   */
  public static ArrayWritable jsonNodeArrayToArrayWritable(JsonNode jsonNode)
  {
    return new ArrayWritable(jsonNodeArrayToList(jsonNode));
  }

  /**
   * Method to take an input json array format string and output an ArrayList
   */
  public static ArrayList<String> jsonNodeArrayToArrayList(JsonNode arrNode)
  {
    /*
    try
    {
      return mapper.readValue(arrNode.toString(), new TypeReference<ArrayList<String>>(){});
    } catch (IOException e)
    {
      e.printStackTrace();
    } finally
    {
      return new ArrayList<>();
    }
    */
    ArrayList<String> retlist = new ArrayList<>();
    Iterator<JsonNode> arrayiter = arrNode.elements();
    while(arrayiter.hasNext())
    {
      JsonNode node = arrayiter.next();
      retlist.add(jacksonSimpleTypeHelper(node).toString());
    }
    return retlist;
  }

  public static void jacksonSimpleTypePutterHelper(JsonNode node, String key, Object value)
  {
    //TODO turn this into a switch, maybe
    if(value instanceof Boolean){
      ((ObjectNode) node).put(key, (Boolean) value);
    }
    else if(value instanceof Integer)
    {
      ((ObjectNode) node).put(key, (Integer) value);
    }
    else if(value instanceof Long)
    {
      ((ObjectNode) node).put(key, (Long) value);
    }
    else if(value instanceof Double)
    {
      ((ObjectNode) node).put(key, (Double) value);
    }
    else if(value instanceof Float)
    {
      ((ObjectNode) node).put(key, (Float) value);
    }
    else if(value instanceof Short)
    {
      ((ObjectNode) node).put(key, (Short) value);
    }
    else if(value instanceof String)
    {
      ((ObjectNode) node).put(key, (String) value);
    }
    else if(value instanceof ArrayList){
      logger.info("I'm making an array node of key " + key + " and value " + value);
      ArrayNode arrayNode = ((ObjectNode) node).putArray(key);
      for(Object element: (ArrayList<Object>)value)
      {
        jacksonSimpleTypePutterHelper(arrayNode, element);
      }
      logger.info("Result: " + node.toString());
    }
    else if(value instanceof Object)
    {
      ((ObjectNode) node).putPOJO(key, value);
    }
    else if(value instanceof byte[])
    {
      ((ObjectNode) node).put(key, (byte[]) value);
    }
    else if(value == null){
      ((ObjectNode) node).putNull(key);
    }
    else ((ObjectNode) node).putNull(key);
  }

  public static void jacksonSimpleTypePutterHelper(ArrayNode node, Object value)
  {
    //TODO turn this into a switch, maybe
    if(value instanceof Boolean){
      node.add((Boolean) value);
    }
    else if(value instanceof Integer)
    {
      node.add((Integer) value);
    }
    else if(value instanceof Long)
    {
      node.add((Long) value);
    }
    else if(value instanceof Double)
    {
      node.add((Double) value);
    }
    else if(value instanceof Float)
    {
      node.add((Float) value);
    }
    else if(value instanceof Short)
    {
      node.add((Short) value);
    }
    else if(value instanceof String)
    {
      node.add((String) value);
    }
    else if(value != null) //instead of instanceof Object, b/c that always hits.
    {
      node.addPOJO(value);
    }
    /* will never be true
    else if(value instanceof byte[])
    {
      node.add((byte[]) value);
    }
    */
    else if(value == null){
      node.addNull();
    }
    else node.addNull();
  }

  public static Object jacksonSimpleTypeHelper(JsonNode node)
  {
    switch(node.getNodeType()){

      case ARRAY:
      case OBJECT:
      case POJO:
        return node.toString();
      case BINARY:
        try
        {
          return node.binaryValue();
        } catch (IOException e)
        {
          e.printStackTrace();
        }
        return null;
      case BOOLEAN:
        return node.booleanValue();
      case MISSING:
      case NULL:
        return null;
      case NUMBER:
        return node.numberValue();
      case STRING:
        return node.textValue();
      default:
        return null;
    }
  }

  /**
   * Method to take an input json array format string and output a String array
   */
  public static String[] jsonNodeArrayToList(JsonNode jsonNode)
  {
    Object[] objarray = jsonNodeArrayToArrayList(jsonNode).toArray();
    return (Arrays.copyOf(objarray, objarray.length, String[].class));
  }

  public static Set<String> jsonGetKeys(JsonNode jsonNode)
  {
    Set<String> keySet = new HashSet<>();
    Iterator<String> fieldnamesiter = jsonNode.fieldNames();
    while(fieldnamesiter.hasNext())
    {
      keySet.add(fieldnamesiter.next());
    }
    return keySet;
  }
}
