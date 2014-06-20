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
import static com.google.common.base.Strings.isNullOrEmpty;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.facebook.presto.hive.CachingHiveMetastore;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

public class KafkaHiveClient {

  com.facebook.presto.hive.HiveClient hiveClient;

  @Inject
  public KafkaHiveClient(KafkaConnectorId connectorId,
      KafkaClientConfig kafkaClientConfig, KafkaPluginConfig kafkaPluginConfig) {
    com.facebook.presto.hive.HiveClientConfig hiveClientConfig = new com.facebook.presto.hive.HiveClientConfig();
    com.facebook.presto.hive.NamenodeStats nameNodestats = new com.facebook.presto.hive.NamenodeStats();
    com.facebook.presto.hive.HdfsConfiguration hdfsConfig = new com.facebook.presto.hive.HdfsConfiguration(
        hiveClientConfig);
    com.facebook.presto.hive.HdfsEnvironment hdfsEnv = new com.facebook.presto.hive.HdfsEnvironment(
        hdfsConfig);
    com.facebook.presto.hive.DirectoryLister dirListener = new com.facebook.presto.hive.HadoopDirectoryLister();
    ExecutorService executorService = Executors
        .newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("kafka-hive-" + connectorId + "-%d").build());
    com.facebook.presto.hive.HiveConnectorId hiveConnectorId = new com.facebook.presto.hive.HiveConnectorId(
        connectorId.toString());
    com.facebook.presto.hive.HiveCluster hiveCluster = KafkaHiveClient
        .createHiveCluster(kafkaPluginConfig, kafkaClientConfig);
    ExecutorService executor = Executors.newFixedThreadPool(
        kafkaClientConfig.getMaxMetastoreRefreshThreads(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("kafka-hive-metastore-" + connectorId + "-%d")
            .build());
    com.facebook.presto.hive.CachingHiveMetastore metastore = new com.facebook.presto.hive.CachingHiveMetastore(
        hiveCluster, executor, hiveClientConfig);
    hiveClient = new com.facebook.presto.hive.HiveClient(hiveConnectorId,
        hiveClientConfig, metastore, nameNodestats, hdfsEnv, dirListener,
        executorService);
  }

  public static com.facebook.presto.hive.HiveCluster createHiveCluster(
      KafkaPluginConfig kafkaPluginConfig, KafkaClientConfig config) {
    URI uri = checkNotNull(kafkaPluginConfig.getMetastoreUri(),
        "metastoreUri is null");
    String scheme = uri.getScheme();
    checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s",
        uri);
    if (scheme.equalsIgnoreCase("thrift")) {
      checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s",
          uri);
      checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s",
          uri);
      HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
      com.facebook.presto.hive.HiveMetastoreClientFactory factory = new com.facebook.presto.hive.HiveMetastoreClientFactory(
          config.getMetastoreSocksProxy(), config.getMetastoreTimeout());
      return new com.facebook.presto.hive.StaticHiveCluster(address, factory);
    }
    throw new IllegalArgumentException("unsupported metastoreUri: " + uri);
  }

  public List<String> listSchemaNames() {
    return this.hiveClient.listSchemaNames();
  }

  public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle) {
    TableHandle newHandle = castToHiveTableHandle(tableHandle);
    return this.hiveClient.getTableMetadata(newHandle);
  }

  private TableHandle castToHiveTableHandle(TableHandle tableHandle) {
    TableHandle newHandle = tableHandle;
    if (tableHandle instanceof KafkaTableHandle) {
      KafkaTableHandle kafkaTbl = (KafkaTableHandle) tableHandle;
      newHandle = new com.facebook.presto.hive.HiveTableHandle(kafkaTbl.getClientId(),
          kafkaTbl.getSchemaName(), kafkaTbl.getTableName());
    }
    return newHandle;
  }

  public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName) {
    TableHandle newHandle = castToHiveTableHandle(tableHandle);
    return this.hiveClient.getColumnHandle(newHandle, columnName);
  }

  public List<SchemaTableName> listTables(String schemaNameOrNull) {
    return this.hiveClient.listTables(schemaNameOrNull);
  }

  public ColumnHandle getSampleWeightColumnHandle(TableHandle tableHandle) {
    final int sampleWightId = 999;
    return new HiveColumnHandle("kafka", HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME,
            sampleWightId, HiveType.INT, sampleWightId, false);
  }

  public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle) {
    TableHandle newHandle = castToHiveTableHandle(tableHandle);
    return this.hiveClient.getColumnHandles(newHandle);
  }

  public ColumnMetadata getColumnMetadata(TableHandle tableHandle,
      ColumnHandle columnHandle) {
    TableHandle newHandle = castToHiveTableHandle(tableHandle);
    return this.hiveClient.getColumnMetadata(newHandle, columnHandle);
  }

  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      SchemaTablePrefix prefix) {
    return this.hiveClient.listTableColumns(prefix);
  }

  public TableHandle createTable(ConnectorTableMetadata tableMetadata) {
    throw new UnsupportedOperationException();
  }

  public void dropTable(TableHandle tableHandle) {
    throw new UnsupportedOperationException();
  }

  public PartitionResult getPartitions(TableHandle tableHandle,
      TupleDomain tupleDomain) {
    TableHandle newHandle = castToHiveTableHandle(tableHandle);
    return this.hiveClient.getPartitions(newHandle, tupleDomain);
  }

  public CachingHiveMetastore getMetastore() {
    return this.hiveClient.getMetastore();
  }

}
