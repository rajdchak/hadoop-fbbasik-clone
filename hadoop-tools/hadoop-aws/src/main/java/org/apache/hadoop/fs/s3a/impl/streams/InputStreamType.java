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

import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.prefetch.PrefetchingInputStreamFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Enum of input stream types.
 * Each enum value contains the factory function actually used to create
 * the factory.
 */
public enum InputStreamType {
  /**
   * The classic input stream.
   */
  Classic("classic", c ->
      new ClassicObjectInputStreamFactory()),

  /**
   * The prefetching input stream.
   */
  Prefetch("prefetch", c ->
      new PrefetchingInputStreamFactory()),
  /**
   * The analytics input stream.
   */
  Analytics("analytics", (c, s3AsyncClient) -> new S3ASeekableInputStreamFactory(s3AsyncClient));

  /**
   * Name.
   */
  private final String name;

  private final BiFunction<Configuration, S3AsyncClient, ObjectInputStreamFactory> factory;
  /**
   * String name.
   * @return the name
   */
  public String getName() {
    return name;
  }

  InputStreamType(String name, Function<Configuration, ObjectInputStreamFactory> factory) {
    this(name, (c, s) -> factory.apply(c));
  }

  InputStreamType(String name, BiFunction<Configuration, S3AsyncClient, ObjectInputStreamFactory> factory) {
    this.name = name;
    this.factory = factory;
  }

  /**
   * Factory constructor.
   * @return the factory associated with this stream type.
   */
  public BiFunction<Configuration, S3AsyncClient, ObjectInputStreamFactory> factory() {
    return factory;
  }

  /**
   * What is the default type?
   */
  public static final InputStreamType DEFAULT_STREAM_TYPE = Classic;



}
