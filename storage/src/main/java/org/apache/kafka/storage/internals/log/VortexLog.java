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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Scheduler;

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class VortexLog {
   // private static final Logger LOGGER = LoggerFactory.getLogger(VortexPartition.class);

    private final VortexInterface vortex = new VortexClient();

    private final TopicPartition topicPartition;
    private LogConfig config;

    // Temporary to store index files. This should be stored in Vortex.
    private final File dir;

    private final String parentDir;

    private long logEndOffset = 0;
    private final VortexLogSegments vortexLogSegments;

    private WriteStream writeStream;

    private Time time;

    private final Scheduler scheduler;

    private final LogDirFailureChannel logDirFailureChannel;

    public static class SplitSegmentResult {
        public final List<VortexLogSegment> deletedSegments;
        public final List<VortexLogSegment> newSegments;

        public SplitSegmentResult(List<VortexLogSegment> deletedSegments, List<VortexLogSegment> newSegments) {
            this.deletedSegments = deletedSegments;
            this.newSegments = newSegments;
        }
    }

    public class Iterator implements java.util.Iterator<VortexRecordBatch> {
        public long currentOffset;
        public int currentBundleOffset = 0;
        public final int fetchIntervalBytes;
        List<VortexRecordBatch> bundles = new ArrayList<>();

        Iterator(long startOffset, int fetchIntervalBytes) {
            this.currentOffset = startOffset;
            this.fetchIntervalBytes = fetchIntervalBytes;
        }
        @Override
        public boolean hasNext() {
            if (currentOffset < logEndOffset() && currentBundleOffset >= bundles.size()) {
                bundles = readRecords(currentOffset, Optional.empty(), fetchIntervalBytes, true);
                currentBundleOffset = 0;
            }
            return currentOffset < logEndOffset() && currentBundleOffset < bundles.size();
        }

        @Override
        public VortexRecordBatch next() {
            VortexRecordBatch next = bundles.get(currentBundleOffset);
            ++currentOffset;
            ++currentBundleOffset;
            return next;
        }
    }

    public VortexLog(File dir, LogConfig config, VortexLogSegments vortexLogSegments, Scheduler scheduler, Time time, TopicPartition topicPartition,
                     LogDirFailureChannel logDirFailureChannel) throws IOException {
        this.topicPartition = topicPartition;
        this.config = config;
        this.dir = dir;
        this.parentDir = dir.getParent();
        this.time = time;
        this.vortexLogSegments = vortexLogSegments;
        this.scheduler = scheduler;
        this.logDirFailureChannel = logDirFailureChannel;
    }

    public TopicPartition topicPartition() {
        return this.topicPartition;
    }

    public Scheduler scheduler() {
        return this.scheduler;
    }

    public LogDirFailureChannel logDirFailureChannel() {
        return this.logDirFailureChannel;
    }

    public File dir() {
        return this.dir;
    }

    public String parentDir() {
        return this.parentDir;
    }

    public File parentDirFile() {
        return new File(this.parentDir);
    }

    public String name() {
        return this.dir.getName();
    }

    public Time time() {
        return this.time;
    }

    public void close() {
    }

    public boolean renameDir(String name) {
        throw new RuntimeException("not implemented");
    }

    public void closeHandlers() {

    }
    public long lastFlushTime() {
        return this.time.milliseconds();
    }

    public void deleteEmptyDir() throws IOException {
        if (vortexLogSegments.nonEmpty()) {
            throw new IllegalStateException("Cannot delete directory that's in use");
        }
        Utils.delete(dir());
    }

    public VortexLogSegments segments() {
        return vortexLogSegments;
    }

    public VortexLogSegment roll(long expectedNextOffset) throws IOException {
        synchronized (this) {
            long newOffset = Math.max(expectedNextOffset, logEndOffset);
            VortexLogSegment activeSegment = vortexLogSegments.activeSegment();
            if (vortexLogSegments.contains(newOffset)) {
                if (activeSegment.baseOffset() == newOffset && activeSegment.size() == 0) {

                }
            } else if (segments().nonEmpty() && newOffset < activeSegment.baseOffset()) {
                throw new RuntimeException("Unexpected");
            } else {
                segments().lastSegment().ifPresent(
                        s -> {
                            try {
                                s.onBecomeInactiveSegment();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            VortexLogSegment newSegment = new VortexLogSegment(this, newOffset, config, dir, time);
            segments().add(newSegment);
            return newSegment;
        }
    }

    public static Collection<VortexLogSegment> replaceSegments(
            VortexLogSegments existingSegments,
            List<VortexLogSegment> newSegments,
            List<VortexLogSegment> oldSegments,
            File dir,
            TopicPartition topicPartition,
            LogConfig config,
            Scheduler scheduler,
            LogDirFailureChannel logDirFailureChannel,
            String logPrefix,
            boolean isRecoveredSwapFile) {
        // TODO
        throw new RuntimeException("Not Implemented");
    }

    public void removeAndDeleteSegments(Iterable<VortexLogSegment> segmentsToDelete,
                                        boolean asyncDelete) {
        throw new RuntimeException("Not Implemented");
    }

    public List<VortexLogSegment> deleteAllSegments() {
        throw new RuntimeException("Not Implemented");
    }

    public static void deleteSegmentFiles(Iterable<VortexLogSegment> segmentsToDelete,
                                   boolean asyncDelete,
                                   File dir,
                                   TopicPartition topicPartition,
                                   LogConfig config,
                                   org.apache.kafka.server.util.Scheduler scheduler,
                                   LogDirFailureChannel logDirFailureChannel,
                                   String logPrefix) {
        throw new RuntimeException("Not Implemented");
    }

    public static SplitSegmentResult splitOverflowedSegment(VortexLogSegment segment,
                                                            VortexLogSegments existingSegments,
                                                            File dir,
                                                            TopicPartition topicPartition,
                                                            LogConfig config,
                                                            Scheduler scheduler,
                                                            LogDirFailureChannel logDirFailureChannel,
                                                            String logPrefix) {
        throw new RuntimeException("Not Implemented");
    }

    public void truncateFullyAndStartAt(long newOffset) {
        throw new RuntimeException("Not Implemented");
    }

    public void updateRecoveryPoint(long newRecovery) {

    }

    public LogConfig config() {
        return config;
    }

    public long unflushedMessages() {
        return 0;
    }

    public void updateConfig(LogConfig newConfig) {
        this.config = newConfig;
    }

    public boolean isFuture() {
        return false;
    }

    public long recoveryPoint() {
        return logEndOffset();
    }

    public void flush(long offset) {
        // Noop
    }

    public void markFlushed(long offset) {
        // Noop
    }

    public void checkIfMemoryMappedBufferClosed() {
        // Noop
    }

    public static VortexLogSegment createNewCleanedSegment(VortexLog vortexLog, File dir, LogConfig logConfig, long baseOffset, Time time) throws IOException {
        return new VortexLogSegment(vortexLog, baseOffset, logConfig, dir, time);
    }

    public Iterable<VortexLogSegment> truncateTo(long targetOffset) {
        throw new RuntimeException("Not Implemented");
    }

    public long logEndOffset() {
        synchronized (this) {
            return logEndOffset;
        }
    }

    public LogOffsetMetadata logEndOffsetMetadata() throws IOException {
        return new LogOffsetMetadata(
                logEndOffset(), vortexLogSegments.activeSegment().baseOffset(),
                vortexLogSegments.activeSegment().sizeInOffsets());
    }

    public  void append(long lastOffset, long maxTimestamp, long shallowOffsetOfMaxTimestamp, MemoryRecords memoryRecords) throws IOException {
        if (memoryRecords.sizeInBytes() == 0) {
            return;
        }

        synchronized (this) {
            if (writeStream == null) {
                writeStream = vortex.getOrCreateWriteStream("foo", this.name(), WriteStream.Type.COMMITTED);
            }

            List<VortexRecords.VortexRecord> vortexRecords = toVortexRecords(memoryRecords, logEndOffset);
         //   if (!dir.toString().contains("cluster_metadata") && !dir.toString().contains("transaction_state"))
          //      System.err.println("APPENDING TO DIR " + this.dir + " RECORDS " + vortexRecords);
            vortexLogSegments.activeSegment().onAppend(
                    logEndOffset,
                    lastOffset,
                    maxTimestamp,
                    shallowOffsetOfMaxTimestamp,
                    memoryRecords.sizeInBytes());
            ProtoRows protoRows = toProtoRows(vortexRecords, logEndOffset);
            // TODO: offset error probably means we need to create a new log.
            vortex.appendRows(writeStream.getName(), logEndOffset, protoRows); // TODO: Handle asynchronousy

            logEndOffset += protoRows.getSerializedRowsCount();
        }
    }

    private FetchDataInfo emptyFetchDataInfo(LogOffsetMetadata fetchOffset, boolean includeAbortedTransactions) {
        Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions = includeAbortedTransactions
                ? Optional.of(Collections.emptyList()) : Optional.empty();
        return new FetchDataInfo(fetchOffset, MemoryRecords.EMPTY, false, abortedTransactions);
    }

    public LogOffsetMetadata convertToOffsetMetadata(long offset) throws IOException {
        FetchDataInfo info = read(offset, 1, false, logEndOffsetMetadata(), false);
        return info.fetchOffsetMetadata;
    }

    public FetchDataInfo read(long startOffset, int maxBytes, boolean atLeastOne,
                              LogOffsetMetadata maxOffsetMetadata, boolean includeAbortedTransactions) throws  IOException {
        synchronized (this) {
            Optional<VortexLogSegment> optSegment = segments().floorSegment(startOffset);

            if (startOffset > logEndOffset || !optSegment.isPresent()) {
                throw new OffsetOutOfRangeException("Offset out of range " + startOffset + " from " + logEndOffset);
            } else if (startOffset == maxOffsetMetadata.messageOffset) {
                return emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTransactions);
            } else if (startOffset > maxOffsetMetadata.messageOffset) {
                return emptyFetchDataInfo(convertToOffsetMetadata(startOffset), includeAbortedTransactions);
            }
            FetchDataInfo fetchDataInfo = null;
            while (fetchDataInfo == null && optSegment.isPresent()) {
                VortexLogSegment vortexLogSegment = optSegment.get();
                Optional<Integer> maxPosition = Optional.empty();
                if (vortexLogSegment.baseOffset() < maxOffsetMetadata.segmentBaseOffset) {
                    maxPosition = Optional.of(vortexLogSegment.sizeInOffsets());
                } else if (vortexLogSegment.baseOffset() == maxOffsetMetadata.segmentBaseOffset && !maxOffsetMetadata.messageOffsetOnly()) {
                    maxPosition = Optional.of(maxOffsetMetadata.relativePositionInSegment);
                }
             //   if (!dir.toString().contains("cluster_metadata") && !dir.toString().contains("transaction_state"))
              //      System.err.println("READING FROM DIR " + dir + " WITH MAX POSITION " + maxPosition);
                VortexLogSegment.FetchDataInfoWithLength fetchDataInfoWithLength =
                        vortexLogSegment.readBatches(startOffset, maxBytes, maxPosition, atLeastOne);
                if (fetchDataInfoWithLength != null) {
                    fetchDataInfo = fetchDataInfoWithLength.fetchDataInfo;
                    if (includeAbortedTransactions) {
                        fetchDataInfo = addAbortedTransactions(startOffset, vortexLogSegment, fetchDataInfo, fetchDataInfoWithLength.length);
                    }
                } else {
                    optSegment = vortexLogSegments.higherSegment(vortexLogSegment.baseOffset());
                }
            }
            return fetchDataInfo != null ? fetchDataInfo : new FetchDataInfo(logEndOffsetMetadata(), MemoryRecords.EMPTY);
        }
    }

    private FetchDataInfo addAbortedTransactions(long startOffset, VortexLogSegment segment, FetchDataInfo fetchInfo, int numRecords) throws IOException {
        OffsetPosition startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
                fetchInfo.fetchOffsetMetadata.relativePositionInSegment);
        long upperBoundOffset = segment.fetchUpperBoundOffset(startOffsetPosition, numRecords).orElse(
                vortexLogSegments.higherSegment(segment.baseOffset()).map(VortexLogSegment::baseOffset).orElse(logEndOffset));

        List<FetchResponseData.AbortedTransaction> abortedTransactions = new ArrayList<>();
        collectAbortedTransactions(startOffset, upperBoundOffset, segment, abortedTransactions);

        return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(abortedTransactions));
    }

    public List<AbortedTxn> collectAbortedTransactions(long logStartOffset, long baseOffset, long upperBoundOffset) {
        Optional<VortexLogSegment> segmentEntry = vortexLogSegments.floorSegment(baseOffset);
        final List<AbortedTxn> allAbortedTxns = new ArrayList<>();
        segmentEntry.ifPresent(segment -> {
            TxnIndexSearchResult searchResult = segment.collectAbortedTxns(logStartOffset, upperBoundOffset);
            allAbortedTxns.addAll(searchResult.abortedTransactions);
        });
        return allAbortedTxns;
    }

    private void collectAbortedTransactions(long startOffset, long upperBoundOffset,
                                           VortexLogSegment startingSegment,
                                            final List<FetchResponseData.AbortedTransaction> abortedTransactions) {
        Function<VortexLogSegment, Boolean> collectTransactions = vls -> {
            TxnIndexSearchResult searchResult = vls.collectAbortedTxns(startOffset, upperBoundOffset);
            searchResult.abortedTransactions.stream()
                    .map(AbortedTxn::asAbortedTransaction)
                    .forEachOrdered(abortedTransactions::add);
            return !searchResult.isComplete;
        };

        if (collectTransactions.apply(startingSegment)) {
            for (VortexLogSegment segment : vortexLogSegments.higherSegments(startingSegment.baseOffset())) {
                if (!collectTransactions.apply(segment)) {
                    return;
                }
            }
        }
    }

    List<VortexRecordBatch> readRecords(long offset, Optional<Long> maxOffset, int maxBytes, boolean atLeastOne) {
        synchronized (this) {
            if (offset > logEndOffset) {
                throw new OffsetOutOfRangeException("Offset out of range " + offset + " from " + logEndOffset);
            } else if (offset == logEndOffset) {
                return Collections.emptyList();
            }
            ProtoRows protoRows = vortex.getRows(writeStream.getName(), offset,
                    maxBytes,
                    maxOffset.orElse(Long.MAX_VALUE),
                    atLeastOne);

            List<VortexRecordBatch> bundles = new ArrayList<>();
            VortexRecordBatch currentBundle = new VortexRecordBatch();
            for (ByteString serializedRow : protoRows.getSerializedRowsList()) {
                VortexRecords.VortexRecord vortexRecord;
                try {
                    vortexRecord = VortexRecords.VortexRecord.parseFrom(serializedRow);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }

                if (vortexRecord.hasStartBatch()) {
                    if (!currentBundle.isEmpty()) {
                        // Close the old bundle and start a new one.
                        bundles.add(currentBundle);
                        currentBundle = new VortexRecordBatch();
                    }
                } else if (currentBundle.isEmpty()) {
                    // TODO: not efficient. This really should be handled via index files the way regular Kafka does.
                    // This also might fail to work if logical offsets and physical offsets diverge due to control records
                    // written to the log. The index should map logical offsets to an appropriate lower bound in the
                    // Vortex stream.
                    ProtoRows startBatchRow = vortex.getRows(writeStream.getName(),
                            vortexRecord.getStartBatchOffset(), 1, Long.MAX_VALUE, true);
                    try {
                        VortexRecords.VortexRecord firstRecordInBatch =
                                VortexRecords.VortexRecord.parseFrom(
                                        startBatchRow.getSerializedRows(0));
                        if (!firstRecordInBatch.hasStartBatch()) {
                            throw new RuntimeException("No start batch!");
                        }
                        vortexRecord = vortexRecord.toBuilder().setStartBatch(firstRecordInBatch.getStartBatch()).build();
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }
                currentBundle.addRecord(new VortexRecordWrapper(vortexRecord, currentBundle));
            }
            if (!currentBundle.isEmpty()) {
                bundles.add(currentBundle);
            }

            return bundles;
        }
    }

    private List<VortexRecords.VortexRecord> toVortexRecords(MemoryRecords memoryRecords, long startOffset) {
        List<VortexRecords.VortexRecord> records = new ArrayList<>();
        for (MutableRecordBatch batch : memoryRecords.batches()) {
        //    if (batch.isCompressed()) {
        //        throw new RuntimeException("We don't yet support compressed batches.");
        //    }
            VortexRecords.StartBatch.Builder startBatchBuilder =
                    VortexRecords.StartBatch.newBuilder();
            VortexRecords.StartBatch startBatch = startBatchBuilder
                    .setMagic(batch.magic())
                    .setPartitionLeaderEpoch(batch.partitionLeaderEpoch())
                    .setIsCompressed(batch.isCompressed())
                    .setIsControl(batch.isControlBatch())
                    .setIsTransactional(batch.isTransactional())
                    .setProducerEpoch(batch.producerEpoch())
                    .setProducerId(batch.hasProducerId() ? batch.producerId() : RecordBatch.NO_PRODUCER_ID)
                    .setTimestampType(batch.timestampType().id)
                    .build();
            for (Record record : batch) {
                records.add(toProto(record, startBatch, startOffset));
                startBatch = null;
            }
        }
        return records;
    }

    private ProtoRows toProtoRows(List<VortexRecords.VortexRecord> records, long startOffset) {
        ProtoRows.Builder protoRows = ProtoRows.newBuilder();
        for (VortexRecords.VortexRecord record : records) {
            protoRows.addSerializedRows(record.toByteString());
        }
        return protoRows.build();
    }

    private VortexRecords.VortexRecord toProto(Record record, VortexRecords.StartBatch startBatch,
                                                                long startOffset) {
        VortexRecords.VortexRecord.Builder builder =
                VortexRecords.VortexRecord.newBuilder();
        if (record.hasKey() && record.key() != null) {
            builder = builder.setKey(ByteString.copyFrom(record.key()));
        }
        if (record.hasValue() && record.value() != null) {
            builder = builder.setValue(ByteString.copyFrom(record.value()));
        }
        builder = builder
                .setOffset(record.offset())
                .setIsCompressed(record.isCompressed())
                .setSequence(record.sequence())
                .setTimestamp(record.timestamp());
        for (int i = 0; i < record.headers().length; ++i) {
            Header header = record.headers()[i];
            builder = builder.addHeader(VortexRecords.Header.newBuilder()
                    .setKey(header.key())
                    .setValue(ByteString.copyFrom(header.value())));
        }
        if (startBatch != null) {
            builder = builder.setStartBatch(startBatch);
        }
        builder = builder.setStartBatchOffset(startOffset);
        return builder.build();
    }

    FileRecords.TimestampAndOffset largestTimestampAfter(long startingPosition) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long shallowOffsetOfMaxTimestamp = -1L;
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        for (VortexRecordBatch bundle : batchesFrom(startingPosition)) {
            if (bundle.maxTimestamp() > maxTimestamp) {
                maxTimestamp = bundle.maxTimestamp();
                shallowOffsetOfMaxTimestamp = bundle.lastOffset();
                leaderEpochOfMaxTimestamp = bundle.getStartBatch().getPartitionLeaderEpoch();
            }
        }

        Optional<Integer> maybeLeaderEpoch = leaderEpochOfMaxTimestamp == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpochOfMaxTimestamp);
        return new FileRecords.TimestampAndOffset(maxTimestamp, shallowOffsetOfMaxTimestamp,
                maybeLeaderEpoch);
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
    }

    public Iterable<VortexRecordBatch> batchesFrom(long startingOffset) {
        final int maxBytes = 2 * 1024 * 1024;
        return () -> new Iterator(startingOffset, maxBytes);
    }

    FileRecords.TimestampAndOffset searchForTimestamp(long targetTimestamp, long startingOffset) {
        for (VortexRecordBatch bundle : batchesFrom(startingOffset)) {
            for (VortexRecordWrapper record : bundle.records()) {
                if (record.timestamp() >= targetTimestamp) {
                    return new FileRecords.TimestampAndOffset(record.timestamp(), record.offset(),
                            maybeLeaderEpoch(bundle.getStartBatch().getPartitionLeaderEpoch()));
                }
            }
        }
        return null;
    }
}
