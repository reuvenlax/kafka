/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class VortexClient implements VortexInterface {
    private static class Stream {
        private final String streamName;
        private final Type type;
        private final List<byte[]> entries = new ArrayList<>();

        Stream(String streamName, Type type) {
            this.streamName = streamName;
            this.type = type;
        }

        WriteStream getWriteStream() {
            return WriteStream.newBuilder()
                    .setName(streamName)
                    .setType(type)
                    .build();
        }

        ApiFuture<AppendRowsResponse> appendRows(long offset, ProtoRows rows) {

            if (offset != -1 && offset != entries.size()) {
                throw new RuntimeException(
                        "Bad append: "
                                + offset
                                + " + for stream "
                                + streamName
                                + " expected "
                                + entries.size());
            }
            entries.addAll(rows.getSerializedRowsList().stream().map(ByteString::toByteArray).collect(Collectors.toList()));
            AppendRowsResponse.Builder responseBuilder = AppendRowsResponse.newBuilder();
            return ApiFutures.immediateFuture(responseBuilder.build());
        }

        ProtoRows getRows(long startIndex, int maxBytes, long maxIndex, boolean atLeastOne) {
            if (startIndex < 0) {
                throw new RuntimeException("Invalid index " + startIndex);
            }
            ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
            int totalBytes = 0;
            boolean seenOne = false;
            for (long i = startIndex; i < entries.size() && i < maxIndex; ++i) {
                if (totalBytes + entries.get((int) i).length > maxBytes && (!atLeastOne || seenOne)) {
                    break;
                }
                ByteString bytes = ByteString.copyFrom(entries.get((int) i));
                VortexRecords.VortexRecord vortexRecord;
                try {
                    vortexRecord = VortexRecords.VortexRecord.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                if (vortexRecord.getOffset() != i) {
                    throw new RuntimeException("Offset doesn't match!");
                }
                protoRowsBuilder.addSerializedRows(ByteString.copyFrom(entries.get((int) i)));
                seenOne = true;
            }
            return protoRowsBuilder.build();
        }
    }

    Map<String, Stream> allStreams = new HashMap<>();
    @Override
    public WriteStream createWriteStream(String tableUrn, Type type) {
        String streamName = UUID.randomUUID().toString();
        Stream stream = new Stream(streamName, type);
        allStreams.put(streamName, stream);
        return stream.getWriteStream();
    }

    @Override
    public WriteStream getWriteStream(String streamName) {
        return allStreams.get(streamName).getWriteStream();
    }

    @Override
    public ApiFuture<AppendRowsResponse> appendRows(String streamName, long offset, ProtoRows rows) {
        return allStreams.get(streamName).appendRows(offset, rows);
    }

    @Override
     public ProtoRows getRows(String streamName, long startOffset, int maxBytes, long maxOffset, boolean atLeastOne) {
        return allStreams.get(streamName).getRows((int) startOffset, maxBytes, maxOffset, atLeastOne);
    }
}
