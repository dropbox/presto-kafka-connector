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

import com.google.common.base.Objects;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class KafkaConnectorId
{
    private final String connectorId;

    public static KafkaConnectorId INSTANCE = null;

    @Inject
    public KafkaConnectorId(String connectorId)
    {
//        System.out.println("connctorId in constructor is:" + connectorId);
//        new Exception().printStackTrace();
//      connectorId = "kafka";
        checkNotNull(connectorId, "connectorId is null");
        checkArgument(!connectorId.isEmpty(), "connectorId is empty");
        this.connectorId = connectorId;
        if (KafkaConnectorId.INSTANCE == null) {
          KafkaConnectorId.INSTANCE = this;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId);
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
        KafkaConnectorId other = (KafkaConnectorId) obj;
        return Objects.equal(this.connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
