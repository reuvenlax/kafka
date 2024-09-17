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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;


class VortexRecordWrapper implements Record {
    VortexRecords.VortexRecord message;
    VortexRecordBatch batch;

    volatile Header[] memoizedHeaders;

    VortexRecordWrapper(VortexRecords.VortexRecord message,
                        VortexRecordBatch batch) {
        this.message = message;
        this.batch = batch;
        memoizedHeaders = null;
    }

    VortexRecords.VortexRecord asProto() {
        return message;
    }

    @Override
    public long offset() {
        return message.getOffset();
    }

    @Override
    public int sequence() {
        return message.getSequence();
    }

    @Override
    public int sizeInBytes() {
        return message.getSerializedSize();
    }

    @Override
    public long timestamp() {
        return message.getTimestamp();
    }

    @Override
    public void ensureValid() {
        return;
    }

    @Override
    public int keySize() {
        return message.getKey().isEmpty() ? -1 : message.getKey().size();
    }

    @Override
    public boolean hasKey() {
        return !message.getKey().isEmpty();
    }

    @Override
    public ByteBuffer key() {
        return message.getKey().isEmpty() ? null : message.getKey().asReadOnlyByteBuffer();
    }

    @Override
    public int valueSize() {
        return message.getValue().isEmpty() ? -1 : message.getValue().size();
    }

    @Override
    public boolean hasValue() {
        return !message.getValue().isEmpty();
    }

    @Override
    public ByteBuffer value() {
        return message.getValue().isEmpty() ? null : message.getValue().asReadOnlyByteBuffer();
    }

    @Override
    public boolean hasMagic(byte magic) {
        if (batch == null || batch.magic() > RecordBatch.MAGIC_VALUE_V1)  {
            return magic >= 2;
        }
        return magic == batch.magic();
    }

    @Override
    public boolean isCompressed() {
        return message.getIsCompressed();
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    @Override
    public Header[] headers() {
        if (memoizedHeaders == null) {
            memoizedHeaders = new Header[message.getHeaderCount()];
            for (int i = 0; i < message.getHeaderCount(); ++i) {
                VortexRecords.Header header = message.getHeader(0);
                memoizedHeaders[i] = new RecordHeader(header.getKey(), header.getValue().toByteArray());
            }
        }
        return memoizedHeaders;
    }
}
