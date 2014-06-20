/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dropbox.presto.kafka;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class KafkaRecordCursor implements RecordCursor
{

    protected static final Logger LOG = Logger.getLogger(KafkaRecordCursor.class);

    final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;
    final static int DEFAULT_TIMEOUT = 60000; // one minute

    private final KafkaSplit split;
    private final List<? extends HiveColumnHandle> columns;
    private final List<? extends ColumnType> columnTypes;
    private long totalBytes;
    private long completedBytes = 0L;
    private long totalRecords = 0L;

    protected JSONObject currentMessage = null;
    private long currentOffset = -1;
    private long startOffset;
    private long nextFetchOffset;
    private long endOffset;

    protected SimpleConsumer consumer = null; /* simple consumer */
    protected FetchRequestBuilder builder = new FetchRequestBuilder();
    protected FetchResponse response = null; /* fetch response */
    protected Iterator<MessageAndOffset> currentResponseIter = null;

    // time stats
    private long splitStartTs = 0L;
    private long totalTimeOnKafka = 0L;
    private long totalTimeOnDecoding = 0L;

    // for sampling
    private JSONObject samplingRate;

    public KafkaRecordCursor(KafkaSplit split,
            List<? extends HiveColumnHandle> columns,
            List<ColumnType> columnTypes)
    {
        this.split = split;
        this.columns = columns;
        this.columnTypes = columnTypes;
        Map<String, Integer> samplingRateMap = new HashMap<String, Integer>();
        samplingRateMap.put(HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME, split.getSampleRate());
        this.samplingRate = new JSONObject(samplingRateMap);

        long startTs = split.getStartTs();
        long endTs = split.getEndTs();
        String clientId = split.getClientId();
        String brokerHost = split.getBrokerHost();
        int brokerPort = split.getBrokerPort();
        this.consumer = new SimpleConsumer(
            brokerHost, brokerPort, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE,
            clientId);
        String topic = split.getTopicName();
        int partitionId = split.getPartitionId();
        this.startOffset = getLastOffset(consumer, topic, partitionId, startTs, clientId);
        this.nextFetchOffset = startOffset;
        this.endOffset = getLastOffset(consumer, topic, partitionId, endTs, clientId);
        // this is wrong since offset only indicates the total amount of messages, not the total amount of size
        this.totalBytes = this.endOffset - startOffset;
        this.completedBytes = 0;
        this.splitStartTs = System.currentTimeMillis();

        System.out.println(String.format("init a record cursor: host: %s topic: %s partition: %d " +
                        "start_ts %d start_offset %d end_ts %d end_offset %d", split.getBrokerHost(),
                split.getTopicName(), split.getPartitionId(), startTs, startOffset, endTs, endOffset));
    }

    private static long getLastOffset(
        kafka.javaapi.consumer.SimpleConsumer consumer, String topic,
        int partition, long whichTime, String clientName) {
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
          partition);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
          whichTime, Integer.MAX_VALUE));
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
          requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
      kafka.javaapi.OffsetResponse response = consumer.getOffsetsBefore(request);

      if (response.hasError()) {
        return 0L;
      }
      long[] offsets = response.offsets(topic, partition);
      return offsets[0];
    }

    @Override
    public long getTotalBytes()
    {
        return this.totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public ColumnType getType(int field)
    {
        return null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (this.currentResponseIter != null
                && this.nextFetchOffset >= this.endOffset)
        {
            return false;
        }

        if (this.currentResponseIter == null
                || !this.currentResponseIter.hasNext())
        {
            long startTs = System.currentTimeMillis();
            fetchMore();
            this.totalTimeOnKafka += (System.currentTimeMillis() - startTs);
        }

        if (this.currentResponseIter == null)
        {
            return false;
        }
        MessageAndOffset currentRecord = this.currentResponseIter.next();
        ByteBuffer msgBuffer = currentRecord.message().payload();
        int size = msgBuffer.limit();
        if (size < 24 || currentRecord.offset() < this.startOffset) {
          return advanceNextPosition();
        }
        long startTs = System.currentTimeMillis();
        this.nextFetchOffset = currentRecord.nextOffset();
        this.currentOffset = currentRecord.offset();
        // TODO we can probably remove the following byte[] and String allocation
        byte[] byteArray = new byte[size];
        msgBuffer.get(byteArray);
        String str = new String(byteArray, 24, byteArray.length - 24);
        this.currentMessage = (JSONObject)JSONValue.parse(str);
        this.currentMessage.merge(samplingRate);
        this.totalRecords ++;
        this.completedBytes += byteArray.length;
        this.totalTimeOnDecoding += (System.currentTimeMillis() - startTs);
        return true;
    }

    private void fetchMore()
    {
        FetchRequest fetchRequest = this.builder
                .clientId(split.getClientId())
                .addFetch(split.getTopicName(), split.getPartitionId(),
                        nextFetchOffset, DEFAULT_BUFFER_SIZE).build();
        response = consumer.fetch(fetchRequest);
        this.currentResponseIter = null;
        if (response != null)
        {
            List<MessageAndOffset> currentResponseList = new ArrayList<MessageAndOffset>();
            for (MessageAndOffset messageAndOffset : response.messageSet(
                    split.getTopicName(), split.getPartitionId()))
            {
                currentResponseList.add(messageAndOffset);
            }
            this.currentResponseIter = currentResponseList.size() > 0 ? currentResponseList.iterator() : null;
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        Object o = getObject(field);
        return o == null ? false : (Boolean)getObject(field);
    }

    @Override
    public long getLong(int field)
    {
        Object o = getObject(field);
        return o == null ? 0L : ((Number)getObject(field)).longValue();
    }

    @Override
    public double getDouble(int field)
    {
        Object o = getObject(field);
        return o == null ? 0 : (Double)getObject(field);
    }

    @Override
    public byte[] getString(int field)
    {
        Object o = getObject(field);
        return o == null ? "null".getBytes() : o.toString().getBytes();
    }

    private Object getObject(int field)
    {
        checkNotNull(this.currentMessage);
        String name = this.columns.get(field).getName();
        return this.currentMessage.get(name);
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        if (consumer != null)
        {
            consumer.close();
        }
        System.out.println("Split: " + this.split + ", total records: " + this.totalRecords + ", total bytes: " +
            this.completedBytes + ", total time spent: " + (System.currentTimeMillis() - this.splitStartTs) +
            ", time in kafka: " + this.totalTimeOnKafka + ", time in decode: " + this.totalTimeOnDecoding);
    }

}
