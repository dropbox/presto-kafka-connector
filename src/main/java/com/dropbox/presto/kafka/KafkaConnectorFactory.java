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
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;

import java.util.Map;

//import org.weakref.jmx.guice.MBeanModule;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;

public class KafkaConnectorFactory implements ConnectorFactory
{
    private final String name;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public KafkaConnectorFactory(String name, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        checkNotNull(config, "config is null");

        try {
            KafkaClientModule kafkaClientModule = new KafkaClientModule(connectorId);

            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new JsonModule(),
                    kafkaClientModule
                );

            Injector injector = app.strictConfig().doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .requireExplicitBindings(false)
                    .setOptionalConfigurationProperties(optionalConfig).initialize();

            KafkaClientConfig clientConfig = KafkaClientConfig.INSTANCE;
            KafkaPluginConfig pluginConfig = KafkaPluginConfig.INSTANCE;
            KafkaConnectorId kafkaConnectorId = KafkaConnectorId.INSTANCE;
            KafkaHiveClient hiveClient = new KafkaHiveClient(kafkaConnectorId,
                clientConfig, pluginConfig);
            KafkaMetadata kafkaMetadata = new KafkaMetadata(hiveClient, kafkaConnectorId);
            KafkaSplitManager kafkaSplitManager = new KafkaSplitManager(hiveClient, kafkaConnectorId, clientConfig);
            KafkaRecordSetProvider kafkaRecordSetProvider = new KafkaRecordSetProvider(kafkaConnectorId);
            KafkaHandleResolver kafkaHandleResolver = new KafkaHandleResolver(kafkaConnectorId);

            ConnectorMetadata connMetadata = new ClassLoaderSafeConnectorMetadata(kafkaMetadata, classLoader);
            ConnectorSplitManager connSplitManager = new ClassLoaderSafeConnectorSplitManager(kafkaSplitManager, classLoader);
            ConnectorRecordSetProvider connRecordSetProvider = new ClassLoaderSafeConnectorRecordSetProvider(kafkaRecordSetProvider, classLoader);
            ConnectorHandleResolver connHandleResolver = new ClassLoaderSafeConnectorHandleResolver(kafkaHandleResolver, classLoader);

            return new KafkaConnector(connMetadata, connSplitManager,
                connRecordSetProvider, connHandleResolver);
            
//            return injector.getInstance(KafkaConnector.class);

//            KafkaMetadata kafkaMetadata = injector.getInstance(KafkaMetadata.class);
//            KafkaSplitManager kafkaSplitManager = injector.getInstance(KafkaSplitManager.class);
//            KafkaRecordSetProvider kafkaRecordSetProvider = injector.getInstance(KafkaRecordSetProvider.class);
//            KafkaHandleResolver kafkaHandleResolver = injector.getInstance(KafkaHandleResolver.class);
//            return new KafkaConnector(kafkaMetadata, kafkaSplitManager,
//                kafkaRecordSetProvider, kafkaHandleResolver);
//            return new KafkaConnector(
//                    new ClassLoaderSafeConnectorMetadata(kafkaMetadata, classLoader),
//                    new ClassLoaderSafeConnectorSplitManager(kafkaSplitManager, classLoader),
//                    new ClassLoaderSafeConnectorRecordSetProvider(kafkaRecordSetProvider, classLoader),
//                    new ClassLoaderSafeConnectorHandleResolver(kafkaHandleResolver, classLoader));
        } catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
    }
}
