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

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class VortexLogSegment {
    private static final Logger LOGGER = LoggerFactory.getLogger(VortexLogSegment.class);

    private final VortexLog vortexLog;

    // TODO: This should be pushed down into the vortex service itself.
    private final LazyIndex<TimeIndex> lazyTimeIndex;

    // TODO: This should be pushed down into the vortex service itself.
    private final LazyIndex<OffsetIndex> lazyOffsetIndex;

    private final TransactionIndex txnIndex;

    // The maximum timestamp and offset we see so far
    // NOTED: the offset is the last offset of batch having the max timestamp.
    private volatile TimestampOffset maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN;
    private long bytesSinceLastIndexEntry = 0;
    private long baseOffset = 0;

    private long lastModifiedTime = -1L;
    private final int indexIntervalBytes;
    private final Time time;

    private OptionalLong rollingBasedTimestamp = OptionalLong.empty();

    // Exists to fake a logfile for users of the segment class
    public class FileRecords extends AbstractRecords {
        public Iterable<VortexRecordBatch> batches() {
            return vortexLog.batchesFrom(baseOffset);
        }

        @Override
        public AbstractIterator<? extends RecordBatch> batchIterator() {
            throw new RuntimeException("Not Implemented");
        }

        @Override
        public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
            throw new RuntimeException("Not Implemented");
        }

        public void readInto(ByteBuffer buffer, int position) throws IOException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public int sizeInBytes() {
            throw new RuntimeException("Not Implemented");
        }

        @Override
        public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
            throw new RuntimeException("Not Implemented");
        }

        public File file() {
            throw new RuntimeException("Not Implemented");
        }

        public org.apache.kafka.common.record.FileRecords.LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
            for (VortexRecordBatch batch : vortexLog.batchesFrom(startingPosition)) {
                long offset = batch.lastOffset();
                if (offset >= targetOffset)
                    return new org.apache.kafka.common.record.FileRecords.LogOffsetPosition(offset, (int) (batch.baseOffset() - baseOffset()), batch.sizeInBytes());
            }
            return null;
        }
    }

    public VortexLogSegment(VortexLog vortexLog, long baseOffset, LogConfig config, File dir, Time time) throws IOException {
        this.vortexLog = vortexLog;
        this.indexIntervalBytes = config.indexInterval;
        this.lazyTimeIndex = LazyIndex.forTime(LogFileUtils.timeIndexFile(dir, baseOffset, ""), baseOffset, config.maxIndexSize);
        this.lazyOffsetIndex = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(dir, baseOffset, ""), baseOffset, config.maxIndexSize);
        this.txnIndex = new TransactionIndex(baseOffset, LogFileUtils.transactionIndexFile(dir, baseOffset, ""));
        this.baseOffset = baseOffset;
        this.time = time;
    }

    public static VortexLogSegment open(VortexLog vortexLog, File dir, long baseOffset, LogConfig config, Time time, int initFileSize, boolean preallocate) throws IOException {
        return open(vortexLog, dir, baseOffset, config, time, false, initFileSize, preallocate, "");
    }

    public static VortexLogSegment open(VortexLog vortexLog, File dir, long baseOffset, LogConfig config, Time time, boolean fileAlreadyExists,
                                  int initFileSize, boolean preallocate, String fileSuffix) throws IOException {
        return new VortexLogSegment(vortexLog, baseOffset, config, dir, time);
    }

    public VortexLog getVortexPartition() {
        return vortexLog;
    }

    public void flush()  {
        // Noop
    }

    public void deleteIfExists() {
        throw new RuntimeException("Not implemented");
    }

    public long getFirstBatchTimestamp() {
        synchronized (this) {
            if (!rollingBasedTimestamp.isPresent()) {
                Iterator<VortexRecordBatch> iter = vortexLog.batchesFrom(baseOffset).iterator();
                if (iter.hasNext()) {
                    rollingBasedTimestamp = OptionalLong.of(iter.next().maxTimestamp());
                }
            }
            if (rollingBasedTimestamp.isPresent() && rollingBasedTimestamp.getAsLong() >= 0)
                return rollingBasedTimestamp.getAsLong();
        }
        return Long.MAX_VALUE;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int size() {
        try {
            return sizeInOffsets();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long readNextOffset() {
        // We are assuming here that we are the last segment.
        return vortexLog.logEndOffset();
    }

    public org.apache.kafka.common.record.FileRecords.LogOffsetPosition translateOffset(long offset) throws IOException {
        OffsetPosition mapping = offsetIndex().lookup(offset);
        return log().searchForOffsetWithSize(offset, Math.max(mapping.position, 0));
    }

    private long positionToOffset(int position) {
        return position + baseOffset;
    }

    // TODO: This is temporary - we'll need the Vortex service itself to implement a time index.
    public TimeIndex timeIndex() throws IOException {
        return lazyTimeIndex.get();
    }

    public OffsetIndex offsetIndex() throws IOException {
        return lazyOffsetIndex.get();
    }

    public File offsetIndexFile() {
        return lazyOffsetIndex.file();
    }

    public File timeIndexFile() {
        return lazyTimeIndex.file();
    }

    public TransactionIndex txnIndex() {
        return txnIndex;
    }

    public void resizeIndexes(int size) throws IOException {
        offsetIndex().resize(size);
        timeIndex().resize(size);
    }

    public int recover(ProducerStateManager producerStateManager, Optional<LeaderEpochFileCache> leaderEpochCache) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    public FileRecords log() {
        return new FileRecords();
    }

    public int truncateTo(long offset) throws IOException {
        throw new RuntimeException("Truncation not supported");
    }

    public boolean shouldRoll(RollParams rollParams) throws IOException {
        return timeIndex().isFull();
    }

    public TimestampOffset readMaxTimestampAndOffsetSoFar() throws IOException {
        if (maxTimestampAndOffsetSoFar == TimestampOffset.UNKNOWN)
            maxTimestampAndOffsetSoFar = timeIndex().lastEntry();
        return maxTimestampAndOffsetSoFar;
    }

    public int sizeInOffsets() throws IOException {
        return (maxTimestampAndOffsetSoFar == TimestampOffset.UNKNOWN) ? 0 : (int) (readMaxTimestampAndOffsetSoFar().offset - baseOffset + 1);
    }

    public Optional<org.apache.kafka.common.record.FileRecords.TimestampAndOffset> findOffsetByTimestamp(long timestampMs, long startingOffset) throws IOException {
        // Get the index entry with a timestamp less than or equal to the target timestamp
        TimestampOffset timestampOffset = timeIndex().lookup(timestampMs);

        // Search the timestamp
        return Optional.ofNullable(vortexLog.searchForTimestamp(timestampMs, Math.max(timestampOffset.offset, startingOffset)));
    }

    public void setLastModified(long ms) throws IOException {
        FileTime fileTime = FileTime.fromMillis(ms);
        Files.setLastModifiedTime(offsetIndexFile().toPath(), fileTime);
        Files.setLastModifiedTime(timeIndexFile().toPath(), fileTime);
    }

    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        return txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }

    public void updateTxnIndex(CompletedTxn completedTxn, long lastStableOffset) throws IOException {
        if (completedTxn.isAborted) {
            LOGGER.trace("Writing aborted transaction {} to transaction index, last stable offset is {}", completedTxn, lastStableOffset);
            txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
        }
    }

    public long maxTimestampSoFar() throws IOException {
        return readMaxTimestampAndOffsetSoFar().timestamp;
    }

    private long shallowOffsetOfMaxTimestampSoFar() throws IOException {
        return readMaxTimestampAndOffsetSoFar().offset;
    }

    public long largestTimestamp() throws IOException {
        return maxTimestampSoFar();
    }

    public OptionalLong fetchUpperBoundOffset(OffsetPosition startOffsetPosition, int fetchSize) throws IOException {
        Optional<OffsetPosition> position = offsetIndex().fetchUpperBoundOffset(startOffsetPosition, fetchSize);
        if (position.isPresent())
            return OptionalLong.of(position.get().offset);
        return OptionalLong.empty();
    }

    public long lastModified() {
        return lastModifiedTime;
    }

    public void sanityCheck(boolean timeIndexFileNewlyCreated) throws IOException {
        txnIndex.sanityCheck();
    }

    public void append(long largestOffset,
                       long largestTimestampMs,
                       long shallowOffsetOfMaxTimestamp,
                       MemoryRecords records) throws IOException {
        vortexLog.append(largestOffset, largestTimestampMs, shallowOffsetOfMaxTimestamp, records);
    }

    void onAppend(long vortexPosition,
            long largestOffset, long largestTimestampMs, long shallowOffsetOfMaxTimestamp,
                  long bytesAppended) throws IOException {
        if (largestTimestampMs > maxTimestampSoFar()) {
            maxTimestampAndOffsetSoFar = new TimestampOffset(largestTimestampMs, shallowOffsetOfMaxTimestamp);
        }
        // append an entry to the index (if needed)
        if (bytesSinceLastIndexEntry > indexIntervalBytes) {
            offsetIndex().append(largestOffset, (int) (vortexPosition - baseOffset));
            timeIndex().maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar());
            bytesSinceLastIndexEntry = 0;
        }
        bytesSinceLastIndexEntry += bytesAppended;
     //   totalSizeBytes += bytesAppended;
        this.lastModifiedTime = time.milliseconds();
    }

    public void onBecomeInactiveSegment() throws IOException {
        timeIndex().maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar(), true);
        timeIndex().trimToValidSize();
    }

    public FetchDataInfo read(long offset, int maxBytes) throws IOException {
        return read(offset, maxBytes, false);
    }

    public FetchDataInfo read(long startOffset, int maxSize, int maxPosition) throws IOException {
        return read(startOffset, maxSize, Optional.of(maxPosition), false);
    }

    public FetchDataInfo read(long offset, int maxBytes,  boolean atLeastOne) throws IOException {
        return read(offset, maxBytes, Optional.empty(), atLeastOne);
    }

    public FetchDataInfo read(long startOffset, int maxBytes, Optional<Integer> maxPositionOpt, boolean atLeastOne) throws IOException {
        return readBatches(startOffset, maxBytes, maxPositionOpt, atLeastOne).fetchDataInfo;
    }


    public static class FetchDataInfoWithLength {
        public final FetchDataInfo fetchDataInfo;
        public final int length;

        public FetchDataInfoWithLength(FetchDataInfo fetchDataInfo, int length) {
            this.fetchDataInfo = fetchDataInfo;
            this.length = length;
        }
    }

    public FetchDataInfoWithLength readBatches(long startOffset, int maxBytes, Optional<Integer> maxPositionOpt, boolean atLeastOne) throws IOException {
        if (startOffset > readMaxTimestampAndOffsetSoFar().offset) {
            return null;
        }

        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, (int) (startOffset - baseOffset()));
        if (maxBytes == 0) {
            return new FetchDataInfoWithLength(new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY), 0);
        }

        Optional<Long> maxOffset = maxPositionOpt.map(this::positionToOffset);
        List<VortexRecordBatch> bundles = vortexLog.readRecords(startOffset, maxOffset, maxBytes, atLeastOne);
        int numKeys = 0;
        for (VortexRecordBatch batch : bundles) {
            for (VortexRecordWrapper wrapper : batch.records()) {
                if (wrapper.hasKey()) {
                    String stringKey = StandardCharsets.UTF_8.decode(wrapper.key()).toString();
                    System.err.println("RECORD: " + stringKey);
                    ++numKeys;
                }
            }
        }
        if (numKeys > 0) {
            System.err.println("Read " + numKeys + " keys");
        }
        int numRecords = bundles.stream().mapToInt(VortexRecordBatch::countOrNull).sum();

        List<ByteBuffer> byteBuffers = new ArrayList<>(bundles.size());
        int totalSize = 0;
        for (VortexRecordBatch bundle : bundles) {
            ByteBuffer byteBuffer = bundle.toByteBuffer();
            totalSize += byteBuffer.limit();
            byteBuffers.add(byteBuffer);
        }

        // Concatenate all of tye ByteBuffers
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        for (ByteBuffer buffer : byteBuffers) {
            byteBuffer.put(buffer);
        }
        byteBuffers.clear();
        byteBuffer.flip();
        MemoryRecords memoryRecords = MemoryRecords.readableRecords(byteBuffer);
        return new FetchDataInfoWithLength(new FetchDataInfo(offsetMetadata, memoryRecords), numRecords);
    }

    public void close() {
        if (maxTimestampAndOffsetSoFar != TimestampOffset.UNKNOWN) {
            Utils.swallow(LOGGER, Level.WARN, "maybeAppend",
                    () -> timeIndex().maybeAppend(
                            maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar(), true));
        }
        Utils.closeQuietly(lazyTimeIndex, "timeIndex", LOGGER);
        Utils.closeQuietly(lazyOffsetIndex, "offsetIndex", LOGGER);
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    void closeHandlers() {
        Utils.swallow(LOGGER, Level.WARN, "offsetIndex", () -> lazyOffsetIndex.closeHandler());
        Utils.swallow(LOGGER, Level.WARN, "timeIndex", () -> lazyTimeIndex.closeHandler());
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    public long timeWaitedForRoll(long now, long messageTimestamp) {
        return 0;
    }

    public long rollJitterMs() {
        return 0;
    }

}
