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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Configuration read from etc/catalog/kudu.properties
 */
@DefunctConfig("kudu.client.default-socket-read-timeout")
public class KuduClientConfig
{
    private static final Duration DEFAULT_OPERATION_TIMEOUT = new Duration(30, TimeUnit.SECONDS);

    private List<String> masterAddresses = ImmutableList.of();
    private Duration defaultAdminOperationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private Duration defaultOperationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private boolean disableStatistics;
    private boolean schemaEmulationEnabled;
    private String schemaEmulationPrefix = "presto::";
    private Duration dynamicFilteringWaitTimeout = new Duration(0, MINUTES);
    private boolean allowLocalScheduling;

    @NotNull
    @Size(min = 1)
    public List<String> getMasterAddresses()
    {
        return masterAddresses;
    }

    @Config("kudu.client.master-addresses")
    public KuduClientConfig setMasterAddresses(List<String> commaSeparatedList)
    {
        this.masterAddresses = commaSeparatedList;
        return this;
    }

    @Config("kudu.client.default-admin-operation-timeout")
    public KuduClientConfig setDefaultAdminOperationTimeout(Duration timeout)
    {
        this.defaultAdminOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultAdminOperationTimeout()
    {
        return defaultAdminOperationTimeout;
    }

    @Config("kudu.client.default-operation-timeout")
    public KuduClientConfig setDefaultOperationTimeout(Duration timeout)
    {
        this.defaultOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultOperationTimeout()
    {
        return defaultOperationTimeout;
    }

    public boolean isDisableStatistics()
    {
        return this.disableStatistics;
    }

    @Config("kudu.client.disable-statistics")
    public KuduClientConfig setDisableStatistics(boolean disableStatistics)
    {
        this.disableStatistics = disableStatistics;
        return this;
    }

    public String getSchemaEmulationPrefix()
    {
        return schemaEmulationPrefix;
    }

    @Config("kudu.schema-emulation.prefix")
    public KuduClientConfig setSchemaEmulationPrefix(String prefix)
    {
        this.schemaEmulationPrefix = prefix;
        return this;
    }

    public boolean isSchemaEmulationEnabled()
    {
        return schemaEmulationEnabled;
    }

    @Config("kudu.schema-emulation.enabled")
    public KuduClientConfig setSchemaEmulationEnabled(boolean enabled)
    {
        this.schemaEmulationEnabled = enabled;
        return this;
    }

    @MinDuration("0ms")
    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("kudu.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public KuduClientConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    public boolean isAllowLocalScheduling()
    {
        return allowLocalScheduling;
    }

    @Config("kudu.allow-local-scheduling")
    @ConfigDescription("Assign Kudu splits to replica host if worker and kudu share the same cluster")
    public KuduClientConfig setAllowLocalScheduling(boolean allowLocalScheduling)
    {
        this.allowLocalScheduling = allowLocalScheduling;
        return this;
    }
}
