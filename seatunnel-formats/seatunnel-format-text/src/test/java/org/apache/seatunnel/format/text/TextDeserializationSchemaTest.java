/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TextDeserializationSchemaTest {

    private SeaTunnelRowType rowType;
    private TextDeserializationSchema schema;

    @BeforeEach
    public void setUp() {
        // Common row type setup for all tests
        rowType =
                new SeaTunnelRowType(
                        new String[] {"city", "order_no", "amount"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .build();
    }

    @Test
    public void testBasicDeserialization() {
        // Test basic line without any special characters
        String line = "New York,ORDER123,100";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("New York", result.get(0));
        Assertions.assertEquals("ORDER123", result.get(1));
        Assertions.assertEquals("100", result.get(2));
    }

    @Test
    public void testEmptyFields() {
        // Test line with empty fields
        String line = "London,,200";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("London", result.get(0));
        Assertions.assertEquals("", result.get(1));
        Assertions.assertEquals("200", result.get(2));
    }

    @Test
    public void testQuotedFields() {
        // Test fields with quotes and embedded delimiters
        String line = "\"San Francisco\",\"ORDER,456\",300";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("\"San Francisco\"", result.get(0));
        Assertions.assertEquals("\"ORDER", result.get(1));
        Assertions.assertEquals("456\"", result.get(2));
    }

    @Test
    public void testEscapedQuotes() {
        // Test fields with escaped quotes
        String line = "\"Los Angeles\",\"ORDER\"\"789\",400";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("\"Los Angeles\"", result.get(0));
        Assertions.assertEquals("\"ORDER\"\"789\"", result.get(1));
        Assertions.assertEquals("400", result.get(2));
    }

    @Test
    public void testInsufficientFields() {
        // Test line with fewer fields than expected
        String line = "Chicago,ORDER999";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("Chicago", result.get(0));
        Assertions.assertEquals("ORDER999", result.get(1));
        Assertions.assertNull(result.get(2));
    }

    @Test
    public void testCustomDelimiter() {
        // Test with a different delimiter
        schema =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter("\\|")
                        .build();

        String line = "Seattle|ORDER888|500";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("Seattle", result.get(0));
        Assertions.assertEquals("ORDER888", result.get(1));
        Assertions.assertEquals("500", result.get(2));
    }

    @Test
    public void testWhitespaceHandling() {
        // Test handling of whitespace around fields
        String line = " Boston , ORDER777 , 600 ";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals(" Boston ", result.get(0));
        Assertions.assertEquals(" ORDER777 ", result.get(1));
        Assertions.assertEquals(" 600 ", result.get(2));
    }

    @Test
    public void testSpecialCharacters() {
        // Test fields containing special characters
        String line = "\"Miami\nFL\",\"ORDER\t123\",700";
        Map<Integer, String> result = schema.splitLineBySeaTunnelRowType(line, rowType, 0);

        Assertions.assertEquals("\"Miami\nFL\"", result.get(0));
        Assertions.assertEquals("\"ORDER\t123\"", result.get(1));
        Assertions.assertEquals("700", result.get(2));
    }
}
