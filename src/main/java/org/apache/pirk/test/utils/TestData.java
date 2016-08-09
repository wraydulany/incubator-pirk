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
package org.apache.pirk.test.utils;

import java.util.ArrayList;

class TestData
{
  public String date;
  public String qname;
  public String src_ip;
  public String dest_ip;
  public ArrayList<Short> qtype;
  public Integer rcode;
  public ArrayList<String> ip;

  public TestData(String date, String qname, String src_ip, String dest_ip, ArrayList<Short> qtype, Integer rcode, ArrayList<String> ip)
  {
    this.date = date;
    this.qname = qname;
    this.src_ip = src_ip;
    this.dest_ip = dest_ip;
    this.qtype = qtype;
    this.rcode = rcode;
    this.ip = ip;
  }
}
