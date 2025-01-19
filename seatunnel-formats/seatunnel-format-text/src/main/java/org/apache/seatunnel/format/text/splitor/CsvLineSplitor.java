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

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CsvLineSplitor implements TextLineSplitor, Serializable {

    /**
     * Split a line into fields based on the given separator and level.
     *
     * <p>This method handles quoted fields by ignoring the separator inside the quotes. If the line
     * can't be split by the separator, the method will fallback to default split. for example:
     * line: "a,b,c", separator: "," -> ["a", "b", "c"] line: "a,"b,c",d, separator: "," ->
     * ["a","b,c","d"] line: "a,"b,"c,d",e",f", separator: "," -> ["a","b,"c,d",e","f"] and note
     * that `b,"c,d",e` is the entire field
     *
     * @param line the line to be split
     * @param separator separator
     * @return an array of fields
     */
    @Override
    public String[] splitLine(String line, String separator) {
        if (StringUtils.isBlank(line)) {
            return new String[0];
        }

        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char currentChar = line.charAt(i);

            if (currentChar == '"') {
                // Handle escaped quotes
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (currentChar == separator.charAt(0) && !inQuotes) {
                // Add field to list if separator is found and not inside quotes
                fields.add(field.toString());
                field.setLength(0);
            } else {
                // Append current character to the current field
                field.append(currentChar);
            }
        }

        // Add the last field to the list
        fields.add(field.toString());
        return fields.toArray(new String[0]);
    }
}
