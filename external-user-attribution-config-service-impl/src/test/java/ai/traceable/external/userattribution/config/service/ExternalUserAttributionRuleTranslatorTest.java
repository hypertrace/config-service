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
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class ExternalUserAttributionRuleTranslatorTest {

  private static final Parser PARSER = JsonFormat.parser();

  private final ExternalUserAttributionRuleTranslator translator =
      new ExternalUserAttributionRuleTranslator();

  @Test
  void translatesCustomRule() {
    assertJsonEquals(
        "{ raw_external_user_attribution_rule: { yaml: \"rule-yaml\"}}",
        translator.translateRules(
            List.of(
                UserAttributionRule.newBuilder()
                    .setData(
                        UserAttributionRuleData.newBuilder()
                            .setCustomData(
                                CustomUserAttributionRuleData.newBuilder().setYaml("rule-yaml")))
                    .build())));
  }

  @Test
  void translatesBasicAuthRule() {
    assertJsonEquals(
        List.of(
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"http.request.header.authorization\",\n"
                + "    type: TYPE_AUTHHEADER\n"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.authorization\",\n"
                + "    type: TYPE_AUTHHEADER\n"
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
                + "    role_paths: [\"$.role\"]"
                + "  }"
                + "}",
            "{"
                + "  transformed_external_user_attribution_rule: {\n"
                + "    attribute_key: \"rpc.request.metadata.authorization\",\n"
                + "    type: TYPE_AUTHHEADER,\n"
                + "    encoding: ENCODING_JWT,"
                + "    id_claims: [\"data-claim\"],"
                + "    id_paths: [\"$.id\"],"
                + "    role_claims: [\"data-claim\"],"
                + "    role_paths: [\"$.role\"]"
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
