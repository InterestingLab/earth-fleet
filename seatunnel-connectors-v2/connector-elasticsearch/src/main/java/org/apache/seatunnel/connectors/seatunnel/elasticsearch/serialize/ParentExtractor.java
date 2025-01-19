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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.function.Function;

@AllArgsConstructor
public class ParentExtractor implements Function<SeaTunnelRow, String>, Serializable {
    private final int fieldIndex;

    @Override
    public String apply(SeaTunnelRow seaTunnelRow) {
        return seaTunnelRow.getField(fieldIndex).toString();
    }

    public static Function<SeaTunnelRow, String> createParentExtractor(
            SeaTunnelRowType rowType, String parentField) {
        if (parentField == null) {
            return row -> null;
        }
        int fieldIndex = rowType.indexOf(parentField);
        return new ParentExtractor(fieldIndex);
    }
}
