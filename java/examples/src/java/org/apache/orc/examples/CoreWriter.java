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

package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.util.StopWatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class CoreWriter {
  public static byte[] binaryValue = new byte[20];

  public static void main(Configuration conf, String[] args) throws IOException {
    System.out.println("Enable new memory check:");
    doBenchmarks(conf);
    System.out.println("Disable new memory check:");
    conf.setLong("orc.stripe.size.check", 0);
    conf.setLong("orc.dictionary.maxSizeInBytes", 0);
    doBenchmarks(conf);
  }

  public static void doBenchmarks(Configuration conf) throws IOException {
    testCheckMemoryCost(conf, 1, 10);
    testCheckMemoryCost(conf, 100, 10);
    testCheckMemoryCost(conf, 1024, 10);
    testCheckMemoryCost(conf, 1024, 100);
    testCheckMemoryCost(conf, 1024, 300);
    testCheckMemoryCost(conf, 1024, 500);
    testCheckMemoryCost(conf, 1024, 1000);
    testCheckMemoryCost(conf, 1024, 5000);
    testCheckMemoryCost(conf, 1024, 10000);
  }

  static void testCheckMemoryCost(Configuration conf, int batchSize, int columnNum) throws IOException {
    File f = new File("my-file.orc");
    if (f.exists()) {
      f.delete();
    }

    StringBuilder schemaBuilder = new StringBuilder("struct<");
    for (int i = 0; i < columnNum; i++) {
      if (i > 0) schemaBuilder.append(",");
      schemaBuilder.append("col").append(i).append(":");
      schemaBuilder.append(i % 2 == 0 ? "int" : "string");
    }
    schemaBuilder.append(">");
    TypeDescription schema = TypeDescription.fromString(schemaBuilder.toString());

    Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch(batchSize);
    Arrays.fill(binaryValue, (byte) 0);

    StopWatch watch = new StopWatch();
    for (int r = 0; r < 10000; ++r) {
      int row = batch.size++;
      for (int c = 0; c < columnNum; c++) {
        if (c % 2 == 0) {
          ((LongColumnVector) batch.cols[c]).vector[row] = r + c;
        } else {
          ((BytesColumnVector) batch.cols[c]).setRef(row, binaryValue, 0, binaryValue.length);
        }
      }
      if (batch.size == batch.getMaxSize()) {
        watch.start();
        writer.addRowBatch(batch);
        watch.stop();
        batch.reset();
      }
    }
    if (batch.size != 0) {
      watch.start();
      writer.addRowBatch(batch);
      watch.stop();
    }
    watch.start();
    writer.close();
    watch.stop();
    System.out.println("Batch Size: " + batchSize + ", column Num: " + columnNum +
        ", took " + watch.now(TimeUnit.MILLISECONDS) + " ms to write 10000 rows, " +
        "flush stripe time: " + writer.getFlushStripeTime() + " ms, " +
        "flush stripe count: " + writer.getFlushStripeCount());
  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
