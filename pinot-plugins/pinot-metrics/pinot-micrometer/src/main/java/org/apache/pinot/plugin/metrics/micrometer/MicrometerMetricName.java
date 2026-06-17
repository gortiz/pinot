/**
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
package org.apache.pinot.plugin.metrics.micrometer;

import java.util.Objects;
import org.apache.pinot.spi.metrics.PinotMetricName;


/// Native Micrometer implementation of {@link PinotMetricName}: holds the Micrometer meter name as a plain string and
/// uses it verbatim as the meter id. The name is produced by the configured {@link MicrometerMetricNamer} (see
/// {@code MicrometerMetricsFactory.makePinotMetricName}); each export leg's naming convention then sanitises it (e.g.
/// the Prometheus leg replaces dots with underscores and adds type suffixes). No tags are attached for now; a later
/// phase introduces tag-first naming.
///
/// <p>Thread-safe (immutable).
public final class MicrometerMetricName implements PinotMetricName {
  private final String _metricName;

  /// @param metricName the Micrometer meter name (already resolved by a {@link MicrometerMetricNamer}, or read back
  ///                   from the registry's meter set in {@code MicrometerMetricsRegistry#allMetrics()})
  public MicrometerMetricName(String metricName) {
    _metricName = metricName;
  }

  @Override
  public String getMetricName() {
    return _metricName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MicrometerMetricName)) {
      return false;
    }
    MicrometerMetricName that = (MicrometerMetricName) obj;
    return Objects.equals(_metricName, that._metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_metricName);
  }

  @Override
  public String toString() {
    return _metricName;
  }
}
