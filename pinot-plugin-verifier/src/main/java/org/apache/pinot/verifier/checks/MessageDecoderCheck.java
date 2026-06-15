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
package org.apache.pinot.verifier.checks;

import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/**
 * A {@link StreamMessageDecoder} is instantiated by {@code RealtimeSegmentDataManager} via
 * {@code PluginManager.createInstance(<decoderClass>)} (bare FQCN, realm walk) when a realtime
 * segment starts consuming. This check exercises that exact path for every shipped decoder.
 *
 * <p>This guards a specific regression class: {@code KafkaJSONMessageDecoder} (in the
 * pinot-kafka-base library, shipped inside the kafka plugin realms) extends
 * {@code JSONMessageDecoder} (in pinot-json, on the host classpath). If pinot-json is missing
 * from the host classpath, the decoder's superclass can't be resolved, the realm walk throws,
 * and realtime consumption fails after retries with an {@code AttemptsExceededException} that is
 * only logged to a file appender — invisible in CI logs. Instantiating the decoder here turns
 * that silent runtime failure into a loud build failure.</p>
 */
public final class MessageDecoderCheck implements Check {
  // {pluginName, StreamMessageDecoder FQCN} pairs. pluginName is the plugins/<name> directory that
  // owns (or, for a -base library class, ships) the decoder; it is only consulted in --strict-realm
  // mode. The default mode resolves the bare FQCN through the realm walk, exactly as the realtime
  // ingestion path does. A list (not a map) because one plugin can ship several decoders.
  private static final String[][] DECODERS = {
      // The regression sentinel: lives in pinot-kafka-base, extends JSONMessageDecoder (pinot-json).
      {"pinot-kafka-3.0", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder"},
      {"pinot-json", "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"},
      {"pinot-csv", "org.apache.pinot.plugin.inputformat.csv.CSVMessageDecoder"},
      {"pinot-avro", "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder"},
      {"pinot-avro", "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder"},
      {"pinot-protobuf", "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder"},
  };

  @Override
  public String name() {
    return "Stream message decoder plugins (StreamMessageDecoder)";
  }

  @Override
  public Outcome run(CheckContext context) {
    int pass = 0;
    int fail = 0;
    for (String[] entry : DECODERS) {
      String pluginName = entry[0];
      String fqcn = entry[1];
      if (!context.targets(fqcn)) {
        continue;
      }
      try {
        Object instance = context.createInstance(pluginName, fqcn);
        if (!(instance instanceof StreamMessageDecoder)) {
          throw new IllegalStateException(fqcn + " is not a StreamMessageDecoder");
        }
        System.out.println("  PASS  " + fqcn);
        if (context.verbose()) {
          for (String line : context.describeCodeSource(instance.getClass())) {
            System.out.println("        " + line);
          }
        }
        pass++;
      } catch (Throwable t) {
        System.out.println("  FAIL  " + fqcn + " — " + t.getClass().getSimpleName() + ": " + t.getMessage());
        if (context.verbose()) {
          t.printStackTrace(System.out);
        }
        fail++;
      }
    }
    return new Outcome(pass, fail);
  }
}
