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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
public class SimpleTextDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private SeaTunnelRowType rowType;

    @Override
    public SeaTunnelRow deserialize(byte[] message) {
        return new SeaTunnelRow(new Object[] {new String(message)});
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message, TablePath tablePath) throws IOException {
        throw new UnsupportedOperationException(
                "Please invoke SimpleTextDeserializationSchema#deserialize(byte[]) instead.");
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return rowType;
    }
}
