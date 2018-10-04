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
package com.kinetica.beam.io;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.BoundedSource;

/** An interface for real or fake implementations of Kinetica. */
public interface KineticaService<T> extends Serializable {
  /**
   * Returns a {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader} that will read from
   * Kinetica using the spec from {@link
   * com.kinetica.beam.io.KineticaIO.KineticaSource}.
   */
  BoundedSource.BoundedReader<T> createReader(KineticaIO.KineticaSource<T> source);

  /** Returns an estimation of the size that could be read. */
  long getEstimatedSizeBytes(KineticaIO.Read<T> spec);

  /** Split a table read into several sources. */
  List<BoundedSource<T>> split(KineticaIO.Read<T> spec, long desiredBundleSizeBytes);

  /** Create a {@link Writer} that writes entities into the Kinetica instance. */
  Writer <T> createWriter(KineticaIO.Write<T> spec);

  /** Writer for an entity. */
  interface Writer<T> extends AutoCloseable {
    /**
     * This method should be synchronous. i.e. the entity is fully
     * stored (and committed) into the Kinetica instance when you exit from this method.
     */
    void write(T entity) throws ExecutionException, InterruptedException;
  }
}
