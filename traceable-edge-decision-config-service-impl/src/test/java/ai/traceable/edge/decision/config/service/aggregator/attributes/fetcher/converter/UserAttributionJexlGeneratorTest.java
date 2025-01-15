package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.KeyMatchOperator;
import ai.traceable.userattribution.config.service.v2.RootRelativeProjection;
import ai.traceable.userattribution.config.service.v2.Template;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import ai.traceable.userattribution.config.service.v2.ValueProjection.Base64Projection;
import ai.traceable.userattribution.config.service.v2.ValueProjection.JwtPayloadClaimProjection;
import ai.traceable.userattribution.config.service.v2.ValueProjection.RegexCaptureGroupProjection;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class UserAttributionJexlGeneratorTest {
  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  private final UserAttributionJexlGenerator userAttributionJexlGenerator =
      new UserAttributionJexlGenerator();

  @Test
  void simpleFieldUserAttribution() {
    UserAttributionRuleData ruleData =
        UserAttributionRuleData.newBuilder()
            .setName("X-Username")
            .setTemplate(Template.TEMPLATE_CUSTOM)
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("x-username")))))
            .build();

    String expectedJexl = "$s.getRequestHeaders().get('x-username')";

    assertEquals(makeDerivationRule(expectedJexl), userAttributionJexlGenerator.convert(ruleData));
  }

  @Test
  void basicRegexUserAttribution() {
    UserAttributionRuleData ruleData =
        UserAttributionRuleData.newBuilder()
            .setName("basic-auth")
            .setTemplate(Template.TEMPLATE_BASIC)
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("authorization")))
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setRegexCaptureGroup(
                                        RegexCaptureGroupProjection.newBuilder()
                                            .setRegex("(?i)Basic:? (.*)")))))
            .build();

    String expectedJexl =
        "$s.getRequestHeaders().get('authorization')" + ".replaceAll(\"(?i)Basic:? (.*)\", \"$1\")";

    assertEquals(makeDerivationRule(expectedJexl), userAttributionJexlGenerator.convert(ruleData));
  }

  @Test
  void basicAuthUserAttribution() {
    UserAttributionRuleData ruleData =
        UserAttributionRuleData.newBuilder()
            .setName("complex-auth")
            .setTemplate(Template.TEMPLATE_BASIC)
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("authorization")))
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setRegexCaptureGroup(
                                        RegexCaptureGroupProjection.newBuilder()
                                            .setRegex("(?i)^Basic:? (.*)")))
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setBase64(Base64Projection.newBuilder()))
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setRegexCaptureGroup(
                                        RegexCaptureGroupProjection.newBuilder()
                                            .setRegex("^([^:]+):?")))))
            .build();
    String expectedJexl =
        "traceableTransformUtils:base64Decode("
            + "$s.getRequestHeaders().get('authorization')"
            + ".replaceAll(\"(?i)^Basic:? (.*)\", \"$1\")"
            + ")"
            + ".replaceAll(\"^([^:]+):?\", \"$1\")";

    assertEquals(makeDerivationRule(expectedJexl), userAttributionJexlGenerator.convert(ruleData));
  }

  @Test
  void testSimpleJwt() {
    UserAttributionRuleData ruleData =
        UserAttributionRuleData.newBuilder()
            .setName("crAPI_user")
            .setTemplate(Template.TEMPLATE_JWT)
            .setRootTokenRule(
                UserAttributionRootTokenRule.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("authorization")))
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setRegexCaptureGroup(
                                        RegexCaptureGroupProjection.newBuilder()
                                            .setRegex("(?i)^Basic:? (.*)")))))
            .setUserIdRule(
                UserAttributionTokenRule.newBuilder()
                    .setRootRelativeProjection(
                        RootRelativeProjection.newBuilder()
                            .addValueProjections(
                                ValueProjection.newBuilder()
                                    .setJwtPayloadClaim(
                                        JwtPayloadClaimProjection.newBuilder()
                                            .setClaimKey("sub")))))
            .build();

    String expectedJexl =
        "traceableTransformUtils:extractJWTPath("
            + "$s.getRequestHeaders().get('authorization')"
            + ".replaceAll(\"(?i)^Basic:? (.*)\", \"$1\")"
            + ", \"sub\", \"PAYLOAD\")";

    assertEquals(makeDerivationRule(expectedJexl), userAttributionJexlGenerator.convert(ruleData));
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testUserAttributionRuleConversion(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    UserAttributionRuleData.Builder inputBuilder = UserAttributionRuleData.newBuilder();
    parser.merge(inputFileStr, inputBuilder);
    DerivationRule output = userAttributionJexlGenerator.convert(inputBuilder.build());
    DerivationRule.Builder expectedOutputBuilder = DerivationRule.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);
    assertEquals(expectedOutputBuilder.build(), output);
  }

  static List<String> getInputFileNames() {
    String folderName =
        UserAttributionJexlGeneratorTest.class.getClassLoader().getResource(INPUT_DIR).getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(queriesFolder.listFiles())
        .map(File::getName)
        .collect(Collectors.toUnmodifiableList());
  }

  private String readResourceFileAsString(String dirName, String fileName) {
    try {
      File file =
          new File(
              this.getClass()
                  .getClassLoader()
                  .getResource(dirName + File.separator + fileName)
                  .toURI());
      return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  DerivationRule makeDerivationRule(String jexlExpression) {
    return DerivationRule.newBuilder()
        .setTransformationConfig(
            DataTransformationConfig.newBuilder()
                .setJexlExpression(
                    JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression))
                .setOutputType(FieldType.FIELD_TYPE_STR))
        .build();
  }
}
