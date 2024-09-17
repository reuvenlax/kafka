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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class VortexRecordBatch implements RecordBatch {
    private final List<VortexRecordWrapper> records;
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    private Optional<Long> offsetOfMaxTimestamp = Optional.empty();
    private int baseSequence = -1;
    private long lastOffset = -1L;

    private int lastSequence = -1;

    private int sizeInBytes = 0;

    private VortexRecords.StartBatch startBatch;

    private long firstOffset;

    private long firstTimestamp;

    public List<VortexRecordWrapper> records() {
        return records;
    }

    public VortexRecords.StartBatch getStartBatch() {
        return startBatch;
    }

    @Override
    public int partitionLeaderEpoch() {
        return startBatch.getPartitionLeaderEpoch();
    }

    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier) {
        return CloseableIterator.wrap(iterator());
    }

    @Override
    public long baseOffset() {
        return firstOffset;
    }

    @Override
    public boolean isControlBatch() {
        return getStartBatch().getIsControl();
    }

    @Override
    public long producerId() {
        return getStartBatch().getProducerId();
    }

    @Override
    public short producerEpoch() {
        return (short) getStartBatch().getProducerEpoch();
    }

    @Override
    public boolean hasProducerId() {
        return RecordBatch.NO_PRODUCER_ID < producerId();
    }

    public long firstTimestamp() {
        return firstTimestamp;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public void ensureValid() {
    }

    @Override
    public long checksum() {
        // TODO: Fix this.
        return 0;
    }

    @Override
    public long maxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public Optional<Long> offsetOfMaxTimestamp() {
        return offsetOfMaxTimestamp;
    }

    @Override
    public TimestampType timestampType() {
        return TimestampType.forId(getStartBatch().getTimestampType());
    }

    @Override
    public long lastOffset() {
        return lastOffset;
    }

    @Override
    public long nextOffset() {
        return lastOffset + 1;
    }

    @Override
    public byte magic() {
        return (byte) getStartBatch().getMagic();
    }

    @Override
    public int baseSequence() {
        return baseSequence;
    }

    @Override
    public int lastSequence() {
        return lastSequence;
    }

    @Override
    public CompressionType compressionType() {
        return CompressionType.NONE;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public Integer countOrNull() {
        return records.size();
    }

    @Override
    public boolean isCompressed() {
        return getStartBatch().getIsCompressed();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(toByteBuffer().duplicate());
    }

    public ByteBuffer toByteBuffer() {
        MemoryRecordsBuilder builder =
                new MemoryRecordsBuilder(
                        ByteBuffer.allocate(getInitialBufferSize(records())),
                        magic(),
                        Compression.NONE,
                        timestampType(),
                        baseOffset(),
                        firstTimestamp(),
                        producerId(),
                        producerEpoch(),
                        baseSequence,
                        isTransactional(),
                        isControlBatch(),
                        getStartBatch().getPartitionLeaderEpoch(),
                        /* writeLimit= */ Integer.MAX_VALUE);

        for (Record record : records()) {
            SimpleRecord simpleRecord = new SimpleRecord(
                    record.timestamp(), record.key(), record.value(), record.headers());
            if  (isControlBatch()) {
                builder.appendControlRecordWithOffset(record.offset(), simpleRecord);
            } else {
                builder.appendWithOffset(record.offset(), simpleRecord);
            }
        }
        return builder.build().buffer();
    }

    private static int getInitialBufferSize(List<VortexRecordWrapper> messages) {
        int size = 0;
        for (VortexRecordWrapper vortexRecordWrapper : messages) {
            size += vortexRecordWrapper.sizeInBytes();
        }
        // Multiply by 1.1 to account for the first resizing of ByteBufferOutputStream
        // http://google3/third_party/java_src/apache_kafka/clients/src/main/java/org/apache/kafka/common/utils/ByteBufferOutputStream.java;l=37;rcl=408129185
        return (int) (size * 1.1);
    }
    @Override
    public boolean isTransactional() {
        return getStartBatch().getIsTransactional();
    }

    @Override
    public OptionalLong deleteHorizonMs() {
        // TODO: fix
        return OptionalLong.empty();
    }

    VortexRecordBatch() {
        this.records = new ArrayList<>();
    }

    boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public java.util.Iterator<Record> iterator() {
        return (Iterator) records.iterator();
    }

    void addRecord(VortexRecordWrapper record) {
        if (this.records.isEmpty()) {
            if (!record.asProto().hasStartBatch()) {
                throw new RuntimeException("Batch started on incorrect boundary.");
            }
            this.startBatch = record.asProto().getStartBatch();
            this.firstOffset = record.offset();
            this.firstTimestamp = record.timestamp();
            this.baseSequence = record.sequence();
        }
        this.records.add(record);
        if (record.timestamp() > this.maxTimestamp) {
            this.maxTimestamp = record.timestamp();
            this.offsetOfMaxTimestamp = Optional.of(record.offset());
        }
        this.lastOffset = record.offset();
        this.lastSequence = record.sequence();
        this.sizeInBytes += record.sizeInBytes();
    }
}
