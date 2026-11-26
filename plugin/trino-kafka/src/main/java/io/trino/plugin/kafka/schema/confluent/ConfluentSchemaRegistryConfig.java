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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy;
import io.trino.spi.connector.SchemaTableName;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ConfluentSchemaRegistryConfig
{
    public enum ConfluentSchemaRegistryAuthType
    {
        NONE,
        BASIC_AUTH,
    }

    private List<String> confluentSchemaRegistryUrls;
    private ConfluentSchemaRegistryAuthType confluentSchemaRegistryAuthType = ConfluentSchemaRegistryAuthType.NONE;
    private int confluentSchemaRegistryClientCacheSize = 1000;
    private EmptyFieldStrategy emptyFieldStrategy = IGNORE;
    private Duration confluentSubjectsCacheRefreshInterval = new Duration(1, SECONDS);
    private Map<SchemaTableName, String> confluentSchemaRegistrySubjectMapping = ImmutableMap.of();

    @Size(min = 1)
    public List<String> getConfluentSchemaRegistryUrls()
    {
        return confluentSchemaRegistryUrls;
    }

    @Config("kafka.confluent-schema-registry-url")
    @ConfigDescription("The url of the Confluent Schema Registry")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryUrls(String confluentSchemaRegistryUrls)
    {
        this.confluentSchemaRegistryUrls = (confluentSchemaRegistryUrls == null) ? null : parseNodes(confluentSchemaRegistryUrls);
        return this;
    }

    public ConfluentSchemaRegistryAuthType getConfluentSchemaRegistryAuthType()
    {
        return confluentSchemaRegistryAuthType;
    }

    @Config("kafka.confluent-schema-registry-auth-type")
    @ConfigDescription("Auth type for logging in Confluent Schema Registry")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryAuthType(ConfluentSchemaRegistryAuthType confluentSchemaRegistryAuthType)
    {
        this.confluentSchemaRegistryAuthType = confluentSchemaRegistryAuthType;
        return this;
    }

    @Min(1)
    @Max(2000)
    public int getConfluentSchemaRegistryClientCacheSize()
    {
        return confluentSchemaRegistryClientCacheSize;
    }

    @Config("kafka.confluent-schema-registry-client-cache-size")
    @ConfigDescription("The maximum number of subjects that can be stored in the Confluent Schema Registry client cache")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryClientCacheSize(int confluentSchemaRegistryClientCacheSize)
    {
        this.confluentSchemaRegistryClientCacheSize = confluentSchemaRegistryClientCacheSize;
        return this;
    }

    public EmptyFieldStrategy getEmptyFieldStrategy()
    {
        return emptyFieldStrategy;
    }

    @Config("kafka.empty-field-strategy")
    @ConfigDescription("How to handle struct types with no fields: ignore, add a marker field named '$empty_field_marker' or fail the query")
    public ConfluentSchemaRegistryConfig setEmptyFieldStrategy(EmptyFieldStrategy emptyFieldStrategy)
    {
        this.emptyFieldStrategy = emptyFieldStrategy;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("30s")
    public Duration getConfluentSubjectsCacheRefreshInterval()
    {
        return confluentSubjectsCacheRefreshInterval;
    }

    @Config("kafka.confluent-subjects-cache-refresh-interval")
    @ConfigDescription("The interval that the topic to subjects cache will be refreshed")
    public ConfluentSchemaRegistryConfig setConfluentSubjectsCacheRefreshInterval(Duration confluentSubjectsCacheRefreshInterval)
    {
        this.confluentSubjectsCacheRefreshInterval = confluentSubjectsCacheRefreshInterval;
        return this;
    }

    public Map<SchemaTableName, String> getConfluentSchemaRegistrySubjectMapping()
    {
        return confluentSchemaRegistrySubjectMapping;
    }

    @Config("kafka.confluent-schema-registry-subject-mapping")
    @ConfigDescription("Comma-separated list of schema.table to actual topic name mappings. Format: schema1.table1:topic1,schema2.table2:topic2")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistrySubjectMapping(String mapping)
    {
        this.confluentSchemaRegistrySubjectMapping = (mapping == null) ? ImmutableMap.of() : parseSubjectMapping(mapping);
        return this;
    }

    private static List<String> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return stream(splitter.split(nodes))
                .map(ConfluentSchemaRegistryConfig::normalizeUrl)
                .collect(toImmutableList());
    }

    private static String normalizeUrl(String value)
    {
        if (value.contains("://")) {
            return value;
        }
        return "https://" + value;
    }

    private static ImmutableMap<SchemaTableName, String> parseSubjectMapping(String mapping)
    {
        requireNonNull(mapping, "mapping is null");

        Splitter entrySplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        Splitter keyValueSplitter = Splitter.on(':').trimResults();

        ImmutableMap.Builder<SchemaTableName, String> builder = ImmutableMap.builder();

        for (String entry : entrySplitter.split(mapping)) {
            List<String> parts = keyValueSplitter.splitToList(entry);
            checkArgument(parts.size() == 2,
                    "Invalid mapping format '%s'. Expected format: schema.table:topic", entry);

            String schemaTable = parts.get(0);
            String topicName = parts.get(1);

            List<String> schemaTableParts = Splitter.on('.').trimResults().splitToList(schemaTable);
            checkArgument(schemaTableParts.size() == 2,
                    "Invalid schema.table format '%s'. Expected format: schema.table", schemaTable);

            String schema = schemaTableParts.get(0);
            String table = schemaTableParts.get(1);

            checkArgument(!schema.isEmpty() && !table.isEmpty(),
                    "Schema and table names cannot be empty in '%s'", schemaTable);
            checkArgument(!topicName.isEmpty(),
                    "Topic name cannot be empty in mapping '%s'", entry);

            SchemaTableName schemaTableName = new SchemaTableName(schema, table);
            builder.put(schemaTableName, topicName);
        }

        return builder.buildOrThrow();
    }
}
