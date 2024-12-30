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

package org.apache.hadoop.fs.s3a.impl.streams;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_ANALYTICS;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_CLASSIC;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_STREAM_TYPE_PREFETCH;

/**
 * Enum of input stream types.
 */
public enum InputStreamType {
  /**
   * The classic input stream.
   */
  Classic(INPUT_STREAM_TYPE_CLASSIC),
  /**
   * The prefetching input stream.
   */
  Prefetch(INPUT_STREAM_TYPE_PREFETCH),
  /**
   * The analytics input stream.
   */
  Analytics(INPUT_STREAM_TYPE_ANALYTICS);

  /**
   * Name.
   */
  private final String name;

  /**
   * String name.
   * @return the name
   */
  public String getName() {
    return name;
  }

  InputStreamType(String name) {
    this.name = name;
  }

  /**
   * What is the default type?
   */
  public static final InputStreamType DEFAULT_STREAM_TYPE = Classic;

}
