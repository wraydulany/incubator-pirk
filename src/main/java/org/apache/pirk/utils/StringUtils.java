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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pirk.schema.data.DataSchema;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;

/* To Replace: */
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
/* To use instead: */
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
  public static MapWritable jsonStringToMapWritable(String jsonString)
  {
    MapWritable value = new MapWritable();
    try
    {
      Map<Object, Object> tempMap = mapper.readValue(jsonString, Map.class);
      for (Map.Entry<Object,Object> entry: tempMap.entrySet())
      {
        Text mapKey = new Text(entry.getKey().toString());
        Text mapValue = new Text();
        if (entry.getValue() != null){
          mapValue.set(entry.getValue().toString());
        }
        value.put(mapKey, mapValue);
      }

    } catch (IOException e)
    {
      logger.error("Unable to parse JSON string: " + jsonString);
      e.printStackTrace();
    }
    return value;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  public static MapWritable jsonStringToMapWritableWithWritableArrayWritable(String jsonString, DataSchema dataSchema)
  {
    //TODO jsonStringToMapWritableWithWritableArrayWritable
    MapWritable value = new MapWritable();
    JSONParser jsonParser = new JSONParser();

    try
    {
      Map<Object, Object> tempMap = mapper.readValue(jsonString, Map.class);
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Map.Entry<Object,Object> entry: tempMap.entrySet())
      //for (Object key : jsonObj.keySet())
      {
        Object key = entry.getKey();
        Text mapKey = new Text(key.toString());
        if (entry.getValue() != null)
        {
          logger.debug("key = " + key.toString());
          if (dataSchema.isArrayElement((String) key))
          {
            WritableArrayWritable mapValue = StringUtils.jsonArrayStringToWritableArrayWritable(entry.getValue().toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            Text mapValue = new Text(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    } catch (JsonParseException e)
    {
      e.printStackTrace();
    } catch (JsonMappingException e)
    {
      e.printStackTrace();
    } catch (IOException e)
    {
      e.printStackTrace();
    }

    return value;
  }

  /**
   * Method to take an input json string and output a MapWritable with arrays as WritableArrayWritable objects
   */
  public static MapWritable jsonStringToMapWritableWithArrayWritable(String jsonString, DataSchema dataSchema)
  {
    //TODO jsonStringToMapWritableWithArrayWritable
    MapWritable value = new MapWritable();
    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        Text mapKey = new Text(key.toString());
        if (jsonObj.get(key) != null)
        {
          logger.debug("key = " + key.toString());
          if (dataSchema.isArrayElement((String) key))
          {
            ArrayWritable mapValue = StringUtils.jsonArrayStringToArrayWritable(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            Text mapValue = new Text(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json string and output a Map<String, Object> with arrays as ArrayList<String> objects and single values as String objects
   */
  public static Map<String,Object> jsonStringToMap(String jsonString, DataSchema dataSchema)
  {
    //TODO jsonStringToMap
    Map<String,Object> value = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();

    try
    {
      value = mapper.readValue(jsonString, HashMap.class);
    } catch (IOException e)
    {
      logger.error("Unable to parse JSON string into Map<String, Object>: " + jsonString);
      e.printStackTrace();
    }

    JSONParser jsonParser = new JSONParser();

    try
    {
      JSONObject jsonObj = (JSONObject) jsonParser.parse(jsonString);
      for (Object key : jsonObj.keySet())
      {
        String mapKey = key.toString();
        if (jsonObj.get(key) != null)
        {
          if (dataSchema.isArrayElement((String) key))
          {
            ArrayList<String> mapValue = StringUtils.jsonArrayStringToArrayList(jsonObj.get(key).toString());
            value.put(mapKey, mapValue);
          }
          else
          {
            value.put(mapKey, jsonObj.get(key).toString());
          }
        }
      }
    } catch (ParseException e)
    {
      logger.warn("Could not json-decode string: " + jsonString, e);
    } catch (NumberFormatException e)
    {
      logger.warn("Could not parse field into number: " + jsonString, e);
    }

    return value;
  }

  /**
   * Method to take an input json array format string and output a WritableArrayWritable
   */
  public static WritableArrayWritable jsonArrayStringToWritableArrayWritable(String jsonString)
  {
    //TODO jsonArrayStringToWritableArrayWritable
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");
    logger.debug("elements = ");
    for (String element : elements)
    {
      logger.debug("element: " + element);
    }

    return new WritableArrayWritable(elements);
  }

  /**
   * Method to take an input json array format string and output an ArrayWritable
   */
  public static ArrayWritable jsonArrayStringToArrayWritable(String jsonString)
  {
    //TODO jsonArrayStringToArrayWritable
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");
    logger.debug("elements = ");
    for (String element : elements)
    {
      logger.debug("element: " + element);
    }

    return new ArrayWritable(elements);
  }

  /**
   * Method to take an input json array format string and output an ArrayList
   */
  public static ArrayList<String> jsonArrayStringToArrayList(String jsonString)
  {
    //TODO jsonArrayStringToArrayList
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    String[] elements = modString.split("\\s*,\\s*");

    return new ArrayList<>(Arrays.asList(elements));
  }

  /**
   * Method to take an input json array format string and output a String array
   */
  public static String[] jsonArrayStringToList(String jsonString)
  {
    //TODO jsonArrayStringToList
    String modString = jsonString.replaceFirst("\\[", "");
    modString = modString.replaceFirst("\\]", "");
    modString = modString.replaceAll("\"", "");
    return modString.split("\\s*,\\s*");
  }
}
