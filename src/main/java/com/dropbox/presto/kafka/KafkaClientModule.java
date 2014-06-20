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
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import io.airlift.configuration.ConfigurationFactory;

import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class KafkaClientModule extends AbstractConfigurationAwareModule
{

    private final String connectorId;
    private ConfigurationFactory configurationFactory;
    private Binder binder;
    private boolean configured = false;

    public KafkaClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

//    public void setBinder(Binder binder)
//    {
//        this.binder = checkNotNull(binder, "binder is null");
//        setup(binder);
//    }

    @Override
    protected void setup(Binder binder) {
      this.binder = binder;
      if (configured || this.binder == null || this.configurationFactory == null) {
        return;
      }

      this.binder.bind(ConfigurationFactory.class).toInstance(
          this.configurationFactory);

      this.binder.bind(KafkaConnectorId.class).toInstance(
              new KafkaConnectorId(connectorId));
//      this.binder.bind(KafkaHandleResolver.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaHiveClient.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaHandleResolver.class).in(Scopes.SINGLETON);

      bindConfig(this.binder).to(KafkaPluginConfig.class);
      bindConfig(this.binder).to(KafkaClientConfig.class);
//      this.binder.bind(KafkaPluginConfig.class).in(Scopes.SINGLETON);
//      this.binder.bind(KafkaClientConfig.class).in(Scopes.SINGLETON);

      this.configured = true;
      this.binder = null;
    }

    @Override
  public synchronized void setConfigurationFactory(
      ConfigurationFactory configurationFactory) {
      System.out.println("setting configuration factory in KafkaClientModule...");
      super.setConfigurationFactory(configurationFactory);
      this.configurationFactory = configurationFactory;
      if (!this.configured && this.binder != null) {
        setup(this.binder);
      }
    }

}
