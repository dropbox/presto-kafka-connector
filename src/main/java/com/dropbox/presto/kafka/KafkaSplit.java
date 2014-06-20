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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class KafkaSplit implements Split
{

    private final String clientId;
    private final String topicName;
    private final int partitionId;
    private final String brokerHost;
    private final int brokerPort;
    private final int sampleRate;
    private final long startTs;
    private final long endTs;
    private final String zookeeper;
    private final int zkSessionTimeout;
    private final int zkConnectTimeout;
    private final HostAddress address;

    @JsonCreator
    public KafkaSplit(@JsonProperty("clientId") String clientId,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("brokerHost") String brokerHost,
            @JsonProperty("brokerPort") int brokerPort,
            @JsonProperty("sampleRate") int sampleRate,
            @JsonProperty("startTs") long startTs,
            @JsonProperty("endTs") long endTs,
            @JsonProperty("zookeeper") String zookeeper,
            @JsonProperty("zkSessionTimeout") int zkSessionTimeout,
            @JsonProperty("zkConnectTimeout") int zkConnectTimeout)
    {
        checkNotNull(clientId, "clientId is null");
        checkNotNull(topicName, "topicName is null");
        checkNotNull(partitionId, "partitionId is null");
        checkNotNull(startTs, "startTs is null");
        checkNotNull(endTs, "endTs is null");
        this.clientId = clientId;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.sampleRate = sampleRate;
        this.startTs = startTs;
        this.endTs = endTs;
        this.zookeeper = zookeeper;
        this.zkSessionTimeout = zkSessionTimeout;
        this.zkConnectTimeout = zkConnectTimeout;
        try {
            InetAddress address = InetAddress.getByName(brokerHost);
            this.address = HostAddress.fromParts(address.getHostAddress(), 8080);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.toString());
        }
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
      return ImmutableList.of(address);
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public String getBrokerHost() {
      return brokerHost;
    }

    @JsonProperty
    public int getBrokerPort() {
      return brokerPort;
    }

    @JsonProperty
    public int getSampleRate() { return sampleRate; }

    @JsonProperty
    public long getStartTs() {
      return startTs;
    }

    @JsonProperty
    public long getEndTs() {
      return endTs;
    }

    @JsonProperty
    public String getZookeeper() {
      return zookeeper;
    }

    @JsonProperty
    public int getZkSessionTimeout() {
      return zkSessionTimeout;
    }

    @JsonProperty
    public int getZkConnectTimeout() {
      return zkConnectTimeout;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder().put("topicName", topicName)
                .put("partitionId", partitionId)
                .put("startTs", startTs)
                .put("endTs", endTs)
                .put("zookeeper", zookeeper)
                .put("zkSessionTimeout", zkSessionTimeout)
                .put("zkConnectTimeout", zkConnectTimeout)
                .build();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).addValue(topicName)
                .addValue(getAddresses())
                .addValue(partitionId)
                .addValue(startTs).addValue(endTs)
                .addValue(zookeeper)
                .addValue(zkSessionTimeout)
                .addValue(zkConnectTimeout).toString();
    }
}
