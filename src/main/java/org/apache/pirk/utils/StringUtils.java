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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.*;
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
    ObjectNode rootNode = mapper.createObjectNode();
    Iterator it = map.entrySet().iterator();
    while(it.hasNext())
    {
      Map.Entry<Writable,Object> pair = (Map.Entry) it.next();
      keyValToJsonNode(rootNode, pair.getKey().toString(), pair.getValue());
    }

    return rootNode.toString();
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

  public static void keyValToJsonNode(JsonNode root, String key, Object value)
  {
    // There has got to be a better way, but this ugly use of a long line of
    // instanceof will work.
    // I think that in the future perhaps if there is serialization needed beyond
    // the primitive types that we should require the user to provide a class object,
    // as it's easy to teach a jackson ObjectMapper to do the right thing if you hand
    // it a properly constructed Class.
    // TODO Implement BigInt and BigDecimal handling, and VIntWritable and VLongWritable
    if(value instanceof String) ((ObjectNode) root).put(key, (String) value);
    else if(value instanceof Text) ((ObjectNode) root).put(key, value.toString());
    else if(value instanceof Boolean) ((ObjectNode) root).put(key, (Boolean) value);
    else if(value instanceof Integer) ((ObjectNode) root).put(key, (Integer) value);
    else if(value instanceof Long) ((ObjectNode) root).put(key, (Long) value);
    else if(value instanceof Double) ((ObjectNode) root).put(key, (Double) value);
    else if(value instanceof Float) ((ObjectNode) root).put(key, (Float) value);
    else if(value instanceof Short) ((ObjectNode) root).put(key, (Short) value);
    else if(value instanceof List){
      logger.info("I'm making an array node of key " + key + " and value " + value);
      ArrayNode arrayNode = ((ObjectNode) root).putArray(key);
      for(Object element: (List<Object>)value)
      {
        listValToJsonArrayNode(arrayNode, element);
      }
      logger.info("Result: " + arrayNode.toString());
    }
    else if(value instanceof BooleanWritable) ((ObjectNode) root).put(key, ((BooleanWritable) value).get());
    else if(value instanceof IntWritable) ((ObjectNode) root).put(key, ((IntWritable) value).get());
    else if(value instanceof LongWritable) ((ObjectNode) root).put(key, ((LongWritable) value).get());
    else if(value instanceof DoubleWritable) ((ObjectNode) root).put(key, ((DoubleWritable) value).get());
    else if(value instanceof FloatWritable) ((ObjectNode) root).put(key, ((FloatWritable) value).get());
    else if(value instanceof ShortWritable) ((ObjectNode) root).put(key, ((ShortWritable) value).get());
    else if(value instanceof ArrayWritable){
      keyValToJsonNode(root, key, ((ArrayWritable) value).toArray());
    }
    // Note that it is of great importance that MapWritable come before Map,
    // as any class that is MapWritable is also Map, and would ping on that if
    // it came first.
    else if(value instanceof MapWritable)
    {
      ObjectNode objNode = mapper.createObjectNode();
      Iterator it = ((Map) value).entrySet().iterator();
      while(it.hasNext())
      {
        Map.Entry<Text, Writable> pair = (Map.Entry)it.next();
        keyValToJsonNode(objNode, pair.getKey().toString(), pair.getValue());
      }
      ((ObjectNode) root).set(key,objNode);
    }
    else if(value instanceof Map)
    {
      ObjectNode objNode = mapper.createObjectNode();
      Iterator it = ((Map) value).entrySet().iterator();
      while(it.hasNext())
      {
        Map.Entry<String, Object> pair = (Map.Entry)it.next();
        keyValToJsonNode(objNode, pair.getKey(), pair.getValue());
      }
      ((ObjectNode) root).set(key,objNode);
    }

    else if(value != null) ((ObjectNode) root).putPOJO(key, value);
    else ((ObjectNode) root).putNull(key);
  }

  public static void listValToJsonArrayNode(JsonNode arrayRoot, Object value)
  {
    // There has got to be a better way, but this ugly use of a long line of
    // instanceof will work.
    // I think that in the future perhaps if there is serialization needed beyond
    // the primitive types that we should require the user to provide a class object,
    // as it's easy to teach a jackson ObjectMapper to do the right thing if you hand
    // it a properly constructed Class.
    if(value instanceof String) ((ArrayNode) arrayRoot).add((String) value);
    else if(value instanceof Boolean) ((ArrayNode) arrayRoot).add((Boolean) value);
    else if(value instanceof Integer) ((ArrayNode) arrayRoot).add((Integer) value);
    else if(value instanceof Long) ((ArrayNode) arrayRoot).add((Long) value);
    else if(value instanceof Double) ((ArrayNode) arrayRoot).add((Double) value);
    else if(value instanceof Float) ((ArrayNode) arrayRoot).add((Float) value);
    else if(value instanceof Short) ((ArrayNode) arrayRoot).add((Short) value);
    else if(value instanceof String) ((ArrayNode) arrayRoot).add((String) value);
    else if(value instanceof List){
      logger.info("I'm making a nested array node of value " + value);
      ArrayNode arrayNode = mapper.createArrayNode();
      for(Object element: (List<Object>)value)
      {
        listValToJsonArrayNode(arrayNode, element);
      }
      ((ArrayNode) arrayRoot).add(arrayNode);
      logger.info("Result: " + arrayNode.toString());
    }
    else if(value instanceof BooleanWritable) ((ArrayNode) arrayRoot).add(((BooleanWritable) value).get());
    else if(value instanceof IntWritable) ((ArrayNode) arrayRoot).add(((IntWritable) value).get());
    else if(value instanceof LongWritable) ((ArrayNode) arrayRoot).add(((LongWritable) value).get());
    else if(value instanceof DoubleWritable) ((ArrayNode) arrayRoot).add(((DoubleWritable) value).get());
    else if(value instanceof FloatWritable) ((ArrayNode) arrayRoot).add(((FloatWritable) value).get());
    else if(value instanceof ShortWritable) ((ArrayNode) arrayRoot).add(((ShortWritable) value).get());
    else if(value instanceof ArrayWritable){
      listValToJsonArrayNode(arrayRoot, ((ArrayWritable) value).toArray());
    }
    // Note that it is of great importance that MapWritable come before Map,
    // as any class that is MapWritable is also Map, and would ping on that if
    // it came first.
    else if(value instanceof MapWritable)
    {
      ObjectNode objNode = ((ArrayNode) arrayRoot).addObject();
      Iterator it = ((Map) value).entrySet().iterator();
      while(it.hasNext())
      {
        Map.Entry<Text, Writable> pair = (Map.Entry)it.next();
        keyValToJsonNode(objNode, pair.getKey().toString(), pair.getValue());
      }
    }
    else if(value instanceof Map)
    {
      ObjectNode objNode = ((ArrayNode) arrayRoot).addObject();
      Iterator it = ((Map) value).entrySet().iterator();
      while(it.hasNext())
      {
        Map.Entry<String, Object> pair = (Map.Entry)it.next();
        keyValToJsonNode(objNode, pair.getKey(), pair.getValue());
      }
    }
    else if(value != null) ((ArrayNode) arrayRoot).addPOJO(value);
    else ((ArrayNode) arrayRoot).addNull();
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
