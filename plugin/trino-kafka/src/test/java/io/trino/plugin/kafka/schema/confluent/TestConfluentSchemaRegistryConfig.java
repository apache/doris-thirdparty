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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.MARK;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryConfig.ConfluentSchemaRegistryAuthType.BASIC_AUTH;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryConfig.ConfluentSchemaRegistryAuthType.NONE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConfluentSchemaRegistryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfluentSchemaRegistryConfig.class)
                .setConfluentSchemaRegistryUrls(null)
                .setConfluentSchemaRegistryAuthType(NONE)
                .setConfluentSchemaRegistryClientCacheSize(1000)
                .setEmptyFieldStrategy(IGNORE)
                .setConfluentSubjectsCacheRefreshInterval(new Duration(1, SECONDS))
                .setConfluentSchemaRegistrySubjectMapping(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.confluent-schema-registry-url", "http://schema-registry-a:8081, http://schema-registry-b:8081")
                .put("kafka.confluent-schema-registry-auth-type", "BASIC_AUTH")
                .put("kafka.confluent-schema-registry-client-cache-size", "1500")
                .put("kafka.empty-field-strategy", "MARK")
                .put("kafka.confluent-subjects-cache-refresh-interval", "2s")
                .put("kafka.confluent-schema-registry-subject-mapping", "default.orders:orders-value")
                .buildOrThrow();

        ConfluentSchemaRegistryConfig expected = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls("http://schema-registry-a:8081, http://schema-registry-b:8081")
                .setConfluentSchemaRegistryAuthType(BASIC_AUTH)
                .setConfluentSchemaRegistryClientCacheSize(1500)
                .setEmptyFieldStrategy(MARK)
                .setConfluentSubjectsCacheRefreshInterval(new Duration(2, SECONDS))
                .setConfluentSchemaRegistrySubjectMapping("default.orders:orders-value");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testHostPortSchemaRegistryUrlDefaultsToHttps()
    {
        ConfluentSchemaRegistryConfig config = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls("schema-registry-a:8081, schema-registry-b:8082");

        assertThat(config.getConfluentSchemaRegistryUrls())
                .containsExactly("https://schema-registry-a:8081", "https://schema-registry-b:8082");
    }

    @Test
    public void testSubjectMappingParsing()
    {
        ConfluentSchemaRegistryConfig config = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistrySubjectMapping("default.orders:orders-value,analytics.users:users-value");

        assertThat(config.getConfluentSchemaRegistrySubjectMapping())
                .containsEntry(new SchemaTableName("default", "orders"), "orders-value")
                .containsEntry(new SchemaTableName("analytics", "users"), "users-value");
    }
}
