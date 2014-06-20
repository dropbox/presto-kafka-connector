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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;

public class KafkaTableHandle implements TableHandle
{

    private final String clientId;
    private final String schemaName;
    private final String tableName;
    private final Map<String, String> props;

    @JsonCreator
    public KafkaTableHandle(@JsonProperty("clientId") String clientId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("props") Map<String, String> props)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.props = props;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Map<String, String> getProps()
    {
        return props;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(clientId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        KafkaTableHandle other = (KafkaTableHandle) obj;
        return Objects.equal(this.clientId, other.clientId)
                && Objects.equal(this.schemaName, other.schemaName)
                && Objects.equal(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return clientId + ":" + schemaName + ":" + tableName;
    }

    public boolean isKafkaTable(String connectorId)
    {
        if (!this.clientId.equalsIgnoreCase(connectorId))
        {
            return false;
        }
        return KafkaTableHandle.kafkaTableMarkPresent(props);
    }

    public static boolean kafkaTableMarkPresent(Map<String, String> props)
    {
        String kafkaMark = props.get(KafkaTableProperties.kafkaTableMarker);
        return kafkaMark != null && kafkaMark.equalsIgnoreCase("true");
    }

}
