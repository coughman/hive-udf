/** * Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the NOTICE file * distributed with this work for additional information * regarding copyright ownership.  The ASF licenses this file * to you under the Apache License, Version 2.0 (the * "License"); you may not use this file except in compliance * with the License.  You may obtain a copy of the License at * *     http://www.apache.org/licenses/LICENSE-2.0 * * Unless required by applicable law or agreed to in writing, software * distributed under the License is distributed on an "AS IS" BASIS, * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. * See the License for the specific language governing permissions and * limitations under the License. */package com.cloudera.hive.udf.examples;import org.apache.hadoop.hive.ql.exec.UDF;/** * This is an example of a Hive UDF that uses the reflective API. */public class Upper extends UDF {		/**	 * This function doesn't override a predefined 	 * @param value - the string to be converted to upper case.	 * @return	 */	public String evaluate(String value) {		return value.toUpperCase();	}}