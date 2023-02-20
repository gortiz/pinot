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
package org.apache.pinot.segment.local.segment.index.fst;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FSTType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class FstIndexTypeTest {

  public class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexDeclaration<FstIndexConfig> expected) {
      Assert.assertEquals(getActualConfig("dimStr", FstIndexType.INSTANCE), expected);
    }

    @Test
    public void oldFieldConfigNull()
        throws JsonProcessingException {
      _tableConfig.setFieldConfigList(null);

      assertEquals(IndexDeclaration.notDeclared(FstIndexType.INSTANCE));
    }

    @Test
    public void oldEmptyFieldConfig()
        throws JsonProcessingException {
      cleanFieldConfig();

      assertEquals(IndexDeclaration.notDeclared(FstIndexType.INSTANCE));
    }

    @Test
    public void oldFieldConfigNotFst()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : []\n"
          + " }");

      assertEquals(IndexDeclaration.notDeclared(FstIndexType.INSTANCE));
    }

    @Test
    public void oldFieldConfigFstNoType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");

      assertEquals(IndexDeclaration.declared(new FstIndexConfig(null)));
    }

    @Test
    public void oldFieldConfigFstNullIndexingType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.setIndexingConfig(null);

      assertEquals(IndexDeclaration.declared(new FstIndexConfig(null)));
    }

    @Test
    public void oldFieldConfigFstExplicitLuceneType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.getIndexingConfig().setFSTIndexType(FSTType.LUCENE);

      assertEquals(IndexDeclaration.declared(new FstIndexConfig(FSTType.LUCENE)));
    }

    @Test
    public void oldFieldConfigFstExplicitNativeType()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"FST\"]\n"
          + " }");
      _tableConfig.getIndexingConfig().setFSTIndexType(FSTType.NATIVE);

      assertEquals(IndexDeclaration.declared(new FstIndexConfig(FSTType.NATIVE)));
    }

    @Test
    public void newNative()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "          \"type\": \"NATIVE\""
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(IndexDeclaration.declared(new FstIndexConfig(FSTType.NATIVE)));
    }

    @Test
    public void newLucene()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "          \"type\": \"LUCENE\""
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(IndexDeclaration.declared(new FstIndexConfig(FSTType.LUCENE)));
    }

    @Test
    public void newEmpty()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"fst\": {\n"
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(IndexDeclaration.declared(new FstIndexConfig(null)));
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.fst(), FstIndexType.INSTANCE, "Standard index should use the same as "
        + "the FstIndexType static instance");
  }
}
