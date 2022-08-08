package ai.traceable.external.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRule;
import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.BasicAuthenticationUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RuleCondition;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class ExternalUserAttributionRuleTranslatorTest {

  private static final Parser PARSER = JsonFormat.parser();
  private static final ExternalUserAttributionConfigServiceConfig config =
      new ExternalUserAttributionConfigServiceConfig(
          ConfigFactory.parseResources("parsingRules.conf"));
  private final ExternalUserAttributionRuleTranslator translator =
      new ExternalUserAttributionRuleTranslator(config);

  @Test
  void translatesCustomRule() {
    assertJsonEquals(
        "{"
            + " raw_external_user_attribution_rule: { yaml: \"rule-yaml\"},"
            + " span_filter: {"
            + "  required_matching_attributes: ["
            + "   {"
            + "     name_predicate: {"
            + "      value: \"http.url\","
            + "      operator: OPERATOR_EQUALS"
            + "     },"
            + "     value_predicate: {"
            + "      value: \"regex1\","
            + "      operator: OPERATOR_MATCHES_REGEX"
            + "     }"
            + "   },"
            + "   {"
            + "     name_predicate: {"
            + "      value: \"http.url\","
            + "      operator: OPERATOR_EQUALS"
            + "     },"
            + "     value_predicate: {"
            + "      value: \"regex2\","
            + "      operator: OPERATOR_MATCHES_REGEX"
            + "     }"
            + "   }"
            + "  ]"
            + " }"
            + " }",
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("rule-yaml")))
                    .setScope(
                        UserAttributionRuleScope.newBuilder()
                            .setCustomScope(
                                UserAttributionRuleScope.CustomScope.newBuilder()
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.newBuilder()
                                            .setUrlMatchRegex("regex1")
                                            .build())
                                    .addUrlScopes(
                                        UserAttributionRuleScope.UrlScope.newBuilder()
                                            .setUrlMatchRegex("regex2")
                                            .build())
                                    .build())
                            .build())
                    .build())));
  }

  @Test
  void translatesBasicAuthRule() {
    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.authorization\",\n"
                + "    type: TYPE_AUTHHEADER,\n"
                + "    attribute_value_parsing_rules: [\n"
                + "    {\n"
                + "      parsing_target: {\n"
                + "        regex_capture_group: \"Basic (.*)\" \n"
                + "      },\n"
                + "      base64_parser: {}\n"
                + "    }]\n"
                + "}\n"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.authorization\",\n"
                + "    type: TYPE_AUTHHEADER,\n"
                + "    attribute_value_parsing_rules: [\n"
                + "    {\n"
                + "      parsing_target: {\n"
                + "        regex_capture_group: \"Basic (.*)\" \n"
                + "      },\n"
                + "      base64_parser: {}\n"
                + "    }]\n"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setBasicAuthenticationData(
                                BasicAuthenticationUserAttributionRuleData.getDefaultInstance()))
                    .build())));
  }

  @Test
  void translatesRequestHeaderRule() {
    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.id-header\",\n"
                + "    type: TYPE_ID\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.id-header\",\n"
                + "    type: TYPE_ID\n"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.newBuilder()
                                    .setUserIdLocation(
                                        HeaderLocation.newBuilder().setHeaderName("id-header"))))
                    .build())));

    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.id-header\",\n"
                + "    type: TYPE_ID\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.id-header\",\n"
                + "    type: TYPE_ID\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.role-header\",\n"
                + "    type: TYPE_ROLE\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.role-header\",\n"
                + "    type: TYPE_ROLE\n"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setRequestHeaderData(
                                RequestHeaderUserAttributionRuleData.newBuilder()
                                    .setUserIdLocation(
                                        HeaderLocation.newBuilder().setHeaderName("id-header"))
                                    .setRoleLocation(
                                        HeaderLocation.newBuilder().setHeaderName("role-header"))))
                    .build())));
  }

  @Test
  void translatesResponseBodyRule() {
    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.response.body\",\n"
                + "    type: TYPE_JSON,\n"
                + "    conditions: [{\n"
                + "      key: \"http.url\",\n"
                + "      regex: \"my-url\"\n"
                + "    }],"
                + "    id_paths: [\"$.somePath\"]"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.response.body\",\n"
                + "    type: TYPE_JSON,\n"
                + "    conditions: [{\n"
                + "      key: \"http.url\",\n"
                + "      regex: \"my-url\"\n"
                + "    }],"
                + "    id_paths: [\"$.somePath\"]"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.newBuilder()
                                    .setCondition(
                                        RuleCondition.newBuilder().setUrlMatchRegex("my-url"))
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.somePath"))))
                    .build())));

    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.response.body\",\n"
                + "    type: TYPE_JSON,\n"
                + "    conditions: [{\n"
                + "      key: \"http.url\",\n"
                + "      regex: \"my-url\"\n"
                + "    }],"
                + "    id_paths: [\"$.someIdPath\"],"
                + "    role_paths: [\"$.someRolePath\"]"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.response.body\",\n"
                + "    type: TYPE_JSON,\n"
                + "    conditions: [{\n"
                + "      key: \"http.url\",\n"
                + "      regex: \"my-url\"\n"
                + "    }],"
                + "    id_paths: [\"$.someIdPath\"],"
                + "    role_paths: [\"$.someRolePath\"]"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setResponseBodyData(
                                ResponseBodyUserAttributionRuleData.newBuilder()
                                    .setCondition(
                                        RuleCondition.newBuilder().setUrlMatchRegex("my-url"))
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.someIdPath"))
                                    .setRoleLocation(
                                        EncodedLocation.newBuilder()
                                            .setJsonPath("$.someRolePath")
                                            .build())))
                    .build())));
  }

  @Test
  void translatesJwtRule() {
    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.authorization\",\n"
                + "    type: TYPE_AUTHHEADER,\n"
                + "    encoding: ENCODING_JWT,"
                + "    id_claims: [\"data-claim\"],"
                + "    id_paths: [\"$.id\"],"
                + "    role_claims: [\"data-claim\"],"
                + "    role_paths: [\"$.role\"],"
                + "    attribute_value_parsing_rules: [\n"
                + "    {\n"
                + "      parsing_target: {\n"
                + "        regex_capture_group: \"Bearer (.*)\" \n"
                + "      },\n"
                + "      jwt_parser: {}\n"
                + "    },\n"
                + "    {\n"
                + "      jwt_parser: {}\n"
                + "    }]\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.authorization\",\n"
                + "    type: TYPE_AUTHHEADER,\n"
                + "    encoding: ENCODING_JWT,\n"
                + "    id_claims: [\"data-claim\"],"
                + "    id_paths: [\"$.id\"],"
                + "    role_claims: [\"data-claim\"],"
                + "    role_paths: [\"$.role\"],"
                + "    attribute_value_parsing_rules: [\n"
                + "    {\n"
                + "      parsing_target: {\n"
                + "        regex_capture_group: \"Bearer (.*)\" \n"
                + "      },\n"
                + "      jwt_parser: {}\n"
                + "    },\n"
                + "    {\n"
                + "      jwt_parser: {}\n"
                + "    }]\n"
                + "  }"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setUserIdClaim("data-claim")
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.id"))
                                    .setRoleClaim("data-claim")
                                    .setRoleLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.role"))))
                    .build())));

    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.cookie\",\n"
                + "    type: TYPE_COOKIE,\n"
                + "    encoding: ENCODING_JWT,\n"
                + "    cookie_name: \"cookie\",\n"
                + "    id_claims: [\"data-claim\"],"
                + "    id_paths: [\"$.id\"],"
                + "    role_claims: [\"data-claim\"],"
                + "    role_paths: [\"$.role\"],"
                + "    attribute_value_parsing_rules: [\n"
                + "    {\n"
                + "      cookie_parser: {}\n"
                + "    }]\n"
                + "}"
                + "}"),
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setJwtData(
                                JwtUserAttributionRuleData.newBuilder()
                                    .setJwtLocation(
                                        HeaderLocation.newBuilder().setCookieName("cookie"))
                                    .setUserIdClaim("data-claim")
                                    .setUserIdLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.id"))
                                    .setRoleClaim("data-claim")
                                    .setRoleLocation(
                                        EncodedLocation.newBuilder().setJsonPath("$.role"))))
                    .build())));
  }

  void assertJsonEquals(List<String> expectedJsons, ExternalUserAttributionRules actualRules) {
    if (expectedJsons.size() != actualRules.getExternalUserAttributionRuleCount()) {
      fail("lists mismatched size");
    }
    for (int i = 0; i < actualRules.getExternalUserAttributionRuleCount(); i++) {
      this.assertJsonEquals(
          expectedJsons.get(i),
          ExternalUserAttributionRules.newBuilder()
              .addExternalUserAttributionRule(actualRules.getExternalUserAttributionRule(i))
              .build());
    }
  }

  @SneakyThrows
  void assertJsonEquals(String expectedJson, ExternalUserAttributionRules actualRule) {
    // Weird API to make it easier to read and verify based on translator api
    if (actualRule.getExternalUserAttributionRuleCount() != 1) {
      fail("should have one rule");
    }

    ExternalUserAttributionRule.Builder expectedRuleBuilder =
        ExternalUserAttributionRule.newBuilder();
    PARSER.merge(expectedJson, expectedRuleBuilder);
    assertEquals(expectedRuleBuilder.build(), actualRule.getExternalUserAttributionRule(0));
  }
}
