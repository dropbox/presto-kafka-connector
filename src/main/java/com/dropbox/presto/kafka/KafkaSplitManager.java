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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.facebook.presto.hive.CachingHiveMetastore;
import com.dropbox.presto.kafka.KafkaHiveClient;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import com.dropbox.presto.kafka.ZKStringSerializer;

public class KafkaSplitManager implements ConnectorSplitManager
{
    private final KafkaHiveClient hiveClient;
    private final String connectorId;
    private final KafkaClientConfig kafkaConfig;

    @Inject
    public KafkaSplitManager(KafkaHiveClient hiveClient,
            KafkaConnectorId connectorId, KafkaClientConfig kafkaConfig)
    {
        this.hiveClient = checkNotNull(hiveClient);
        this.connectorId = checkNotNull(connectorId).toString();
        this.kafkaConfig = checkNotNull(kafkaConfig);
    }

    @Override
    public String getConnectorId()
    {
        return this.connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof KafkaTableHandle
                && ((KafkaTableHandle) tableHandle).isKafkaTable(connectorId);
    }

    private static SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof KafkaTableHandle,
                "tableHandle is not an instance of HiveTableHandle");
        return ((KafkaTableHandle) tableHandle).getSchemaTableName();
    }

    private CachingHiveMetastore metastore()
    {
        return this.hiveClient.getMetastore();
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle,
            TupleDomain tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        Table table;
        try {
            table = metastore().getTable(tableName.getSchemaName(),
                    tableName.getTableName());
            Map<String, String> props = table.getParameters();
            if (!KafkaTableHandle.kafkaTableMarkPresent(props))
            {
                throw new RuntimeException("table not a kakfa mapped table.");
            }
        } catch (NoSuchObjectException e) {
            throw new RuntimeException(tableName + " not found.");
        }

        return hiveClient.getPartitions(tableHandle, tupleDomain);
    }

    @Override
    public SplitSource getPartitionSplits(TableHandle tableHandle,
            List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");

        Partition partition = Iterables.getFirst(partitions, null);
        if (partition != null)
        {
          checkArgument(partition instanceof HivePartition,
              "Partition must be a hive partition");
        }

        SchemaTableName tableName = getTableName(tableHandle);
        Table table = null;
        Iterable<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = null;
        try
        {
            table = metastore().getTable(tableName.getSchemaName(),
                    tableName.getTableName());
            if (table.getPartitionKeys() != null && table.getPartitionKeys().size() > 0) {
              List<String> partitionNames = new ArrayList<String> (Lists.transform(
                  partitions, partitionIdGetter()));
              Collections.sort(partitionNames, Ordering.natural().reverse());
              hivePartitions = getPartitions(table, tableName, partitionNames);
            }
        } catch (NoSuchObjectException e) {
          throw new RuntimeException(tableName + " not found.");
        } catch (MetaException e) {
          throw new RuntimeException(tableName + " not found.");
        }

        return new KafkaSplitSourceProvider(connectorId, table, hivePartitions,
                kafkaConfig).get();
    }

    public static Function<Partition, String> partitionIdGetter()
    {
        return new Function<Partition, String>()
        {
            @Override
            public String apply(Partition input)
            {
                return input.getPartitionId();
            }
        };
    }

    private Iterable<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(
            final Table table, final SchemaTableName tableName,
            List<String> partitionNames) throws NoSuchObjectException,
            MetaException
    {
        if (partitionNames.equals(ImmutableList.of(UNPARTITIONED_ID)))
        {
            throw new RuntimeException("Got unpartitioned table.");
        }

        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = metastore()
                .getPartitionsByNames(tableName.getSchemaName(),
                        tableName.getTableName(), partitionNames);

        return partitions;
    }
}
