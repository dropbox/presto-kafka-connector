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
import static com.google.common.collect.Lists.transform;

import java.util.List;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.inject.Inject;

public class KafkaRecordSetProvider implements ConnectorRecordSetProvider
{

    private final String connectorId;

    @Inject
    public KafkaRecordSetProvider(KafkaConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null")
                .toString();
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof KafkaSplit
                && ((KafkaSplit) split).getClientId().equals(connectorId);
    }

    @Override
    public RecordSet getRecordSet(Split split,
            List<? extends ColumnHandle> columns)
    {
        return new KafkaRecordSet((KafkaSplit) split, columns);
    }

    static class KafkaRecordSet implements RecordSet
    {

        private KafkaSplit split;
        private List<? extends HiveColumnHandle> columns;
        private final List<ColumnType> columnTypes;

        public KafkaRecordSet(KafkaSplit split,
                List<? extends ColumnHandle> columns)
        {
            this.split = split;
            checkNotNull(columns, "columns is null");
            this.columns = transform(columns,
                    HiveColumnHandle.hiveColumnHandle());
            this.columnTypes = transform(this.columns,
                    HiveColumnHandle.nativeTypeGetter());
        }

        public List<ColumnType> getColumnTypes()
        {
            return this.columnTypes;
        }

        public RecordCursor cursor()
        {
            return new KafkaRecordCursor(split, columns, columnTypes);
        }

    }
}
