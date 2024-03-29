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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CsvLineSplitor implements TextLineSplitor {
    private CSVFormat format;

    @Override
    public String[] spliteLine(String line, String splitor) {
        if (Objects.isNull(format)) {
            this.format = CSVFormat.DEFAULT.withDelimiter(splitor.charAt(0));
        }
        CSVParser parser = null;
        // Method to parse the line into CSV with the given separator
        try {
            // Create CSV parser
            parser = CSVParser.parse(line, format);
            // Parse the CSV records
            List<String> res = new ArrayList<>();
            for (CSVRecord record : parser.getRecords()) {
                for (String value : record) {
                    res.add(value);
                }
            }
            parser.close();
            return res.toArray(new String[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return new String[0];
        } finally {
            if (Objects.nonNull(parser)) {
                try {
                    parser.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
