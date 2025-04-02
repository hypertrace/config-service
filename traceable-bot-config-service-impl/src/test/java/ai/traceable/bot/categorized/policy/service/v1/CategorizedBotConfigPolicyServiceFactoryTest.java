package ai.traceable.bot.categorized.policy.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigPolicyServiceFactoryTest {

  @Test
  void testBindings() {
    final Channel mockChannel = mock(Channel.class);
    final ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    final GrpcChannelRegistry grpcChannelRegistry = mock(GrpcChannelRegistry.class);
    final Config mockConfig = mock(Config.class);

    doReturn(mock(ManagedChannel.class))
        .when(grpcChannelRegistry)
        .forPlaintextAddress("localhost", 50061);

    when(mockConfig.getConfig("entity.fetcher.cache"))
        .thenReturn(
            ConfigFactory.parseString(
                "  service.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterWriteDuration = 1h\n"
                    + "  }\n"
                    + "  api.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterAccessDuration = 1h\n"
                    + "  }\n"));
    when(mockConfig.getConfig("entity.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "config {\n"
                    + "    host = localhost\n"
                    + "    port = 50061\n"
                    + "    timeout = 10s\n"
                    + "  }\n"
                    + "  attributeMap {\n"
                    + "    service.id = \"SERVICE.id\"\n"
                    + "    service.name = \"SERVICE.name\"\n"
                    + "    service.environment = \"SERVICE.environment\"\n"
                    + "    environment.id = \"ENVIRONMENT.id\"\n"
                    + "    environment.name = \"ENVIRONMENT.name\"\n"
                    + "  }"));
    when(mockConfig.getConfig("bot.config.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "default.policies = [\n"
                    + "    {\n"
                    + "      \"id\": \"bd35c18f-4287-458c-93dc-0150d9bc7b93\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Crawlers\",\n"
                    + "        \"description\": \"Default Policy created for Crawlers\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Crawlers\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"d1b42153-70d9-4e19-89e5-baad262123b2\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Monitoring and Development Bots\",\n"
                    + "        \"description\": \"Default Policy created for Monitoring and Development Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Monitoring and Development Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"794adce2-32af-42ce-84c6-f78798d27cd9\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Social Media and Content Bots\",\n"
                    + "        \"description\": \"Default Policy created for Social Media and Content Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Social Media and Content Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"a5f2f9b4-4bda-46d7-9a70-319b2371311c\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Marketing and SEO Bots\",\n"
                    + "        \"description\": \"Default Policy created for Marketing and SEO Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Marketing and SEO Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"1fbd03b8-b386-42f4-a681-ba9289f2aece\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"Search and Indexing Bots\",\n"
                    + "        \"description\": \"Default Policy created for Search and Indexing Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"Search and Indexing Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"id\": \"03ed4bfd-6314-4792-b525-01dd162c5a51\",\n"
                    + "      \"categorizedBotPolicyDetails\": {\n"
                    + "        \"name\": \"E-commerce and Financial Bots\",\n"
                    + "        \"description\": \"Default Policy created for E-commerce and Financial Bots\",\n"
                    + "        \"botScopes\": [{\n"
                    + "          \"botClassification\": {\n"
                    + "            \"category\": \"E-commerce and Financial Bots\"\n"
                    + "          }\n"
                    + "        }],\n"
                    + "        \"categorizedBotPolicyActionConfig\": {\n"
                    + "          \"botAction\": \"CATEGORIZED_BOT_ACTION_MONITOR\"\n"
                    + "        },\n"
                    + "        \"isDefaultPolicy\": true\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]"));

    assertDoesNotThrow(
        () ->
            CategorizedBotConfigPolicyServiceFactory.build(
                mockChannel, mockEventGenerator, grpcChannelRegistry, mockConfig));
  }
}
