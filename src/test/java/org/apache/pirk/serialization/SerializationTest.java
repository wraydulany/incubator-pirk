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
package org.apache.pirk.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SerializationTest
{

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static JsonSerializer jsonSerializer;
  private static JavaSerializer javaSerializer;

  @BeforeClass
  public static void setUp() throws Exception
  {
    jsonSerializer = new JsonSerializer();
    javaSerializer = new JavaSerializer();
  }

  @Test
  public void testJsonSerDe() throws Exception
  {
    File tempFile = folder.newFile("test-json-serialize");
    FileOutputStream fos = new FileOutputStream(tempFile);
    DummyRecord dummyRecord = new DummyRecord();

    jsonSerializer.write(fos, dummyRecord);

    FileInputStream fis = new FileInputStream(tempFile);
    Object deserializedDummyObject = jsonSerializer.read(fis, DummyRecord.class);
    Assert.assertEquals(dummyRecord, deserializedDummyObject);
  }

  @Test
  public void testJavaSerDe() throws Exception
  {
    File tempFile = folder.newFile("test-java-serialize");
    FileOutputStream fos = new FileOutputStream(tempFile);
    DummyRecord dummyRecord = new DummyRecord();

    javaSerializer.write(fos, new DummyRecord());

    FileInputStream fis = new FileInputStream(tempFile);
    Object deserializedDummyObject = javaSerializer.read(fis, DummyRecord.class);
    Assert.assertTrue(deserializedDummyObject.equals(dummyRecord));
  }

  private static class DummyRecord implements Serializable, Storable
  {
    private int id;
    private String message;
    private long seed = 100L;

    DummyRecord()
    {
      this.id = (new Random(seed)).nextInt(5);
      this.message = "The next message id is " + id;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public String getMessage()
    {
      return message;
    }

    public void setMessage(String message)
    {
      this.message = message;
    }

    @Override
    public String toString()
    {
      return "DummyRecord{" + "id=" + id + ", message='" + message + '\'' + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      DummyRecord that = (DummyRecord) o;
      return id == that.id && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id, message);
    }
  }

}
