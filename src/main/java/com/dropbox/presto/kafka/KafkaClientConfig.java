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

import java.util.concurrent.TimeUnit;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.net.HostAndPort;


public class KafkaClientConfig
{

    private String zookeeper;
    private int zookeeperSessionTimeout = 5000;
    private int zookeeperConnectTimeout = 5000;
    private int maxMetastoreRefreshThreads = 100;
    private HostAndPort metastoreSocksProxy;

    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private Duration metastoreRefreshInterval = new Duration(2, TimeUnit.MINUTES);

    public static KafkaClientConfig INSTANCE = null;

    public KafkaClientConfig() {
      if(INSTANCE == null) {
        INSTANCE = this;
      }
    }

    @Config("kafka.zookeeper")
    public KafkaClientConfig setZookeeper(String zookeeper)
    {
        this.zookeeper = zookeeper;
        return this;
    }

    @NotNull
    public String getZookeeper()
    {
        return this.zookeeper;
    }

    @Config("kafka.zookeeper.session.timeout")
    public KafkaClientConfig setZookeeperSessionTimeout(int sessionTimeout)
    {
        this.zookeeperSessionTimeout = sessionTimeout;
        return this;
    }

    @Min(1000)
    public int getZookeeperSessionTimeout()
    {
        return zookeeperSessionTimeout;
    }

    @Config("kafka.zookeeper.connect.timeout")
    public KafkaClientConfig setZookeeperConnectTimeout(int connectTimeout)
    {
        this.zookeeperConnectTimeout = connectTimeout;
        return this;
    }

    @Min(1000)
    public int getZookeeperConnectTimeout()
    {
        return zookeeperConnectTimeout;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public KafkaClientConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public KafkaClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public KafkaClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @Config("hive.metastore-refresh-interval")
    public KafkaClientConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = metastoreRefreshInterval;
        return this;
    }

}
