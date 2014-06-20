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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.facebook.presto.hive.CachingHiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.inject.Inject;

import com.dropbox.presto.kafka.KafkaHiveClient;

public class KafkaMetadata implements ConnectorMetadata
{

    private final KafkaHiveClient hiveClient;
    private final String connectorId;

    @Inject
    public KafkaMetadata(KafkaHiveClient hiveClient, KafkaConnectorId connectorId)
    {
        this.hiveClient = hiveClient;
        this.connectorId = checkNotNull(connectorId).toString();
    }

    private CachingHiveMetastore metastore()
    {
        return hiveClient.getMetastore();
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof KafkaTableHandle
                && ((KafkaTableHandle) tableHandle).isKafkaTable(connectorId);
    }

    @Override
    public List<String> listSchemaNames()
    {
        return hiveClient.listSchemaNames();
    }

    @Override
    public KafkaTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try
        {
            Table tbl = metastore().getTable(tableName.getSchemaName(),
                    tableName.getTableName());
            Map<String, String> props = tbl.getParameters();
            if (!KafkaTableHandle.kafkaTableMarkPresent(props))
            {
                throw new UnsupportedOperationException("not kafka table");
            }
            return new KafkaTableHandle(connectorId, tableName.getSchemaName(),
                    tableName.getTableName(), props);
        } catch (NoSuchObjectException e)
        {
            // table was not found
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        return hiveClient.getTableMetadata(tableHandle);
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle,
            String columnName)
    {
        return hiveClient.getColumnHandle(tableHandle, columnName);
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        return hiveClient.listTables(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        return hiveClient.getSampleWeightColumnHandle(tableHandle);
    }

    @Override
    public boolean canCreateSampledTables()
    {
        return false;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return hiveClient.getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return hiveClient.getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            SchemaTablePrefix prefix)
    {
        return hiveClient.listTableColumns(prefix);
    }

    @Override
    public TableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        return hiveClient.createTable(tableMetadata);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        hiveClient.dropTable(tableHandle);
    }

    @Override
    public boolean canHandle(OutputTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public OutputTableHandle beginCreateTable(
            ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle,
            Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

}
