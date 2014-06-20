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

import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class KafkaSplitSourceProvider
{

    private final String connectorId;
    private final Table table;
    private final Iterable<Partition> hivePartitions;
    private final KafkaClientConfig kafkaConfig;

    public KafkaSplitSourceProvider(String connectorId, Table table,
            Iterable<Partition> hivePartitions, KafkaClientConfig kafkaConfig)
    {
        this.connectorId = connectorId;
        this.table = table;
        this.hivePartitions = hivePartitions;
        this.kafkaConfig = kafkaConfig;
    }

    public SplitSource get()
    {
        return new KafkaSplitSource(this.connectorId, this.table,
                this.hivePartitions, this.kafkaConfig);
    }

    static class KafkaSplitSource implements SplitSource
    {

        private final String connectorId;

        private List<Split> computedSplits;
        private int fetchedIndex;

        KafkaSplitSource(String connectorId, Table table,
                Iterable<Partition> hivePartitions,
                KafkaClientConfig kafkaConfig)
        {
            this.connectorId = connectorId;
            this.fetchedIndex = 0;
            this.computedSplits = new ArrayList<Split>();
            String zookeeper = kafkaConfig.getZookeeper();
            int zkSessionTimeout = kafkaConfig.getZookeeperSessionTimeout();
            int zkConnectionTimeout = kafkaConfig.getZookeeperConnectTimeout();

            Map<String, String> tblProps = table.getParameters();
            String tableTopic = tblProps.get(KafkaTableProperties.kafkaTopicName);

            long splitRange = getDefault(tblProps, KafkaTableProperties.kafkaSplitRange, 60 * 60 * 1000);
            long scanRange = getDefault(tblProps, KafkaTableProperties.kafkaJobRange, 24 * 60 * 60 * 1000);
            int sampleRate = (int) getDefault(tblProps, KafkaTableProperties.kafkaTableSampleRate, 100);

            ZkClient zkclient = new ZkClient(zookeeper, zkSessionTimeout,
                    zkConnectionTimeout, new ZKStringSerializer());

            TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk(tableTopic, zkclient);
            List<PartitionMetadata> mds = scala.collection.JavaConversions.asJavaList(metadata.partitionsMetadata());

            List<long[]> offsetList = null;
            // if the table is partitioned, look at each partition and
            // determine the data to look at.
            List<FieldSchema> partCols = table.getPartitionKeys();
            if (partCols != null && partCols.size() > 0)
            {
                offsetList = generateTsOffsetsFromPartitions(hivePartitions, tblProps, splitRange, partCols);
            } else
            {
                // we will set the table property so that all the the queries hit here.
                offsetList = generateTsOffsetsNoPartitions(scanRange, mds.size());
            }

            for (PartitionMetadata md : mds)
            {
                Broker broker = md.leader().get();
                for (long[] offsets : offsetList)
                {
                    long startTs = offsets[0];
                    long endTs = offsets[1];
                    KafkaSplit split = new KafkaSplit(connectorId,
                            tableTopic, md.partitionId(),
                            broker.host(), broker.port(),
                            sampleRate,
                            startTs, endTs, zookeeper,
                            zkSessionTimeout, zkConnectionTimeout);
                    this.computedSplits.add(split);
                }
            }
        }

        private long getDefault(Map<String, String> tblProps, String prop, long defaultVal)
        {
            String val = tblProps.get(prop);
            if (val != null)
            {
                return Long.parseLong(val);
            } else {
                return defaultVal;
            }
        }

        private List<long[]> generateTsOffsetsFromPartitions(Iterable<Partition> hivePartitions,
                                                             Map<String, String> tblProps, final long splitRange,
                                                             List<FieldSchema> partCols)
        {
            List<long[]> offsetList = new ArrayList<long[]>();
            String tsParName = tblProps.get(KafkaTableProperties.kafkaPartTsColumn);
            int tsPartColIndex = -1;
            for (int i = 0; i < partCols.size(); i++)
            {
                FieldSchema partCol = partCols.get(i);
                String colName = partCol.getName();
                if (colName.equalsIgnoreCase(tsParName))
                {
                    tsPartColIndex = i;
                    break;
                }
            }

            if (tsPartColIndex == -1)
            {
                throw new RuntimeException("Can not find partition timestamp column:" + tsParName);
            }

            Iterator<Partition> partitionIter = hivePartitions.iterator();
            while (partitionIter.hasNext())
            {
                Partition nextPart = partitionIter.next();
                long[] offsets = new long[2];
                List<String> values = nextPart.getValues();
                String start = values.get(tsPartColIndex);
                offsets[0] = Long.parseLong(start) * 1000;
                offsets[1] = offsets[0] + splitRange;
                offsetList.add(offsets);
            }
            return offsetList;
        }

        private List<long[]> generateTsOffsetsNoPartitions(final long scanRange,
                                                           final int partitionNum)
        {
            // we want to increase the number of splits so that it can achieve maximum parallelism
            // the idle number would be splits == cores
            // TODO make this configerable
            final int numHosts = 40;
            final int numCorePerHosts = 32;
            final int splitsWanted = numHosts * numCorePerHosts;
            final long start = System.currentTimeMillis() - scanRange;
            long secondsPerSplit = scanRange / (splitsWanted / partitionNum);

            List<long[]> offsetList = new ArrayList<long[]>();
            for (int i = 0; i < splitsWanted / partitionNum; ++i) {
                long[] offsets = new long[2];
                offsets[0] = start + secondsPerSplit * i;
                offsets[1] = start + secondsPerSplit * (i + 1);
                offsetList.add(offsets);
            }
            offsetList.get(offsetList.size() - 1)[1] = OffsetRequest.LatestTime();
            return offsetList;
        }

        @Override
        public String getDataSourceName()
        {
            return connectorId;
        }

        @Override
        public List<Split> getNextBatch(int maxSize)
                throws InterruptedException
        {
            // shortcut to return everything in one call.
            if (maxSize >= this.computedSplits.size() && this.fetchedIndex == 0)
            {
                this.fetchedIndex = this.computedSplits.size();
                return this.computedSplits;
            }
            // compute splits to return
            int left = this.computedSplits.size() - this.fetchedIndex;
            int fetchSize = maxSize;
            if (left < maxSize)
            {
                fetchSize = left;
            }
            List<Split> ret = new ArrayList<Split>(fetchSize);
            while (fetchedIndex < this.computedSplits.size())
            {
                ret.add(this.computedSplits.get(fetchedIndex));
                this.fetchedIndex++;
            }
            return ret;
        }

        @Override
        public boolean isFinished()
        {
            return this.fetchedIndex == this.computedSplits.size();
        }
    }
}
