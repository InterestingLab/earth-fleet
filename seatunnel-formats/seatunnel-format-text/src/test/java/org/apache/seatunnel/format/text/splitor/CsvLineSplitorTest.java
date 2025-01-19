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

package org.apache.seatunnel.format.text.splitor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CsvLineSplitorTest {

    private CsvLineSplitor splitor;

    @BeforeEach
    public void setUp() {
        splitor = new CsvLineSplitor();
    }

    @Test
    public void testBasicSplit() {
        // Test basic CSV splitting
        String line = "New York,London,Tokyo";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(new String[] {"New York", "London", "Tokyo"}, result);
    }

    @Test
    public void testEmptyFields() {
        // Test handling of empty fields
        String line = "Paris,,Berlin,";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(new String[] {"Paris", "", "Berlin", ""}, result);
    }

    @Test
    public void testQuotedFields() {
        // Test fields with quotes containing separators
        String line = "\"Los Angeles\",\"San Francisco,CA\",Seattle";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(
                new String[] {"Los Angeles", "San Francisco,CA", "Seattle"}, result);
    }

    @Test
    public void testQuotedFields2() {
        // Test fields with quotes containing separators
        String quotedLine = "Shanghai,\"123,456,789\",200";
        String[] quotedResult = splitor.splitLine(quotedLine, ",");

        Assertions.assertEquals("Shanghai", quotedResult[0]);
        Assertions.assertEquals("123,456,789", quotedResult[1]);
        Assertions.assertEquals("200", quotedResult[2]);
    }

    @Test
    public void testEscapedQuotes() {
        // Test handling of escaped quotes
        String line = "\"Chicago\",\"New \"\"York\"\" City\",Boston";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(
                new String[] {"Chicago", "New \"York\" City", "Boston"}, result);
    }

    @Test
    public void testComplexQuotes() {
        // Test complex quoting scenarios with simpler cases
        String[] testCases = {
            // Basic quoted field
            "\"Miami\",\"Vegas\",\"Phoenix\"",
            // Field with internal comma
            "\"Miami,FL\",\"Las Vegas\",\"Phoenix\""
        };

        String[][] expectedResults = {
            {"Miami", "Vegas", "Phoenix"},
            {"Miami,FL", "Las Vegas", "Phoenix"},
        };

        for (int i = 0; i < testCases.length; i++) {
            String[] result = splitor.splitLine(testCases[i], ",");
            Assertions.assertArrayEquals(
                    expectedResults[i], result, "Failed on test case " + i + ": " + testCases[i]);
        }
    }

    @Test
    public void testCustomSeparator() {
        // Test custom separator
        String line = "Dallas|Houston|Austin";
        String[] result = splitor.splitLine(line, "|");
        Assertions.assertArrayEquals(new String[] {"Dallas", "Houston", "Austin"}, result);
    }

    @Test
    public void testEmptyInput() {
        // Test empty input
        String line = "";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(new String[] {}, result);

        // Test whitespace input
        line = "   ";
        result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(new String[] {}, result);
    }

    @Test
    public void testSpecialCharacters() {
        // Test special characters
        String line = "Portland\nOR,Denver\tCO,Atlanta\rGA";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(
                new String[] {"Portland\nOR", "Denver\tCO", "Atlanta\rGA"}, result);
    }

    @Test
    public void testMixedQuotesAndSpecialChars() {
        // Test mixed quotes and special characters
        String line = "\"San Jose\nCA\",\"Oakland,\tCA\",\"Sacramento\rCA\"";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(
                new String[] {"San Jose\nCA", "Oakland,\tCA", "Sacramento\rCA"}, result);
    }

    @Test
    public void testSingleQuote() {
        // Test handling of unclosed quotes
        String line = "Detroit,\"Minneapolis,St Paul";
        String[] result = splitor.splitLine(line, ",");
        Assertions.assertArrayEquals(new String[] {"Detroit", "Minneapolis,St Paul"}, result);
    }
}
