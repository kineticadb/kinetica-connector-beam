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

package com.kinetica.beam.test;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * This shared set of options is used so that the full suite of IO tests can be run in one pass -
 * if a test tries to read TestPipelineOptions, it must be able to understand all the options
 * that were passed on the command line.
 */
public interface IOTestPipelineOptions extends TestPipelineOptions {

  /* Kinetica */
  @Description("URL for Kinetica head node (scheme://host name/ip address:port)")
  @Default.String("http://localhost:9191")
  String getKineticaURL();
  void setKineticaURL(String value);

  @Description("Username for Kinetica login")
  @Default.String("admin")
  String getKineticaUsername();
  void setKineticaUsername(String value);

  @Description("Password for Kinetica login")
  @Default.String("Kinetica1!")
  String getKineticaPassword();
  void setKineticaPassword(String value);

  @Description("Kinetica table name")
  @Default.String("scientists")
  String getKineticaTable();
  void setKineticaTable(String value);

  @Description("Min number of simultanous requests on read")
  @Default.String("test")
  Integer getMinNumberOfSplits();
  void setMinNumberOfSplits(Integer value);
  
}
