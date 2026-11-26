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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.schema.KafkaSchemaRegistryClientPropertiesProvider;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestConfluentModule
{
    @Test
    public void testBasicAuthPropertiesIncludedInSchemaRegistryClientProperties()
    {
        ConfluentSchemaRegistryConfig config = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls("schema-registry.example.com:8081")
                .setConfluentSchemaRegistryAuthType(ConfluentSchemaRegistryConfig.ConfluentSchemaRegistryAuthType.BASIC_AUTH);
        BasicAuthConfig basicAuthConfig = new BasicAuthConfig()
                .setConfluentSchemaRegistryUsername("user1")
                .setConfluentSchemaRegistryPassword("secret1");

        Set<SchemaRegistryClientPropertiesProvider> providers = ImmutableSet.of(
                new KafkaSchemaRegistryClientPropertiesProvider(new ConfluentSchemaRegistryBasicAuth(basicAuthConfig)));
        java.util.Map<String, Object> merged = ConfluentModule.buildSchemaRegistryClientProperties(config, providers);

        assertThat(merged)
                .containsEntry("basic.auth.credentials.source", "USER_INFO")
                .containsEntry("basic.auth.user.info", "user1:secret1")
                .containsEntry("schema.registry.basic.auth.credentials.source", "USER_INFO")
                .containsEntry("schema.registry.basic.auth.user.info", "user1:secret1");
    }
}
