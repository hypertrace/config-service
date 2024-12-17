package ai.traceable.ratelimiting.service.v2.rules.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverter;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RateLimitingEdgeDecisionConverterTest {
  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  static RateLimitingEdgeDecisionConverter converter;

  @BeforeAll
  static void setup() {
    Injector injector = Guice.createInjector(new RateLimitingConditionModule());
    Set<RateLimitingConditionConverter> conditionConverters =
        injector.getInstance(Key.get(new TypeLiteral<Set<RateLimitingConditionConverter>>() {}));
    converter = new RateLimitingEdgeDecisionConverter(conditionConverters);
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testEvaluateRule(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    RateLimitingRule.Builder rateLimitingRuleBuilder = RateLimitingRule.newBuilder();
    parser.merge(inputFileStr, rateLimitingRuleBuilder);
    EdgeDecisionEngineConfig output = converter.convert(List.of(rateLimitingRuleBuilder.build()));
    EdgeDecisionEngineConfig.Builder expectedOutputBuilder = EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);
    assertEquals(expectedOutputBuilder.build(), output);
  }

  static List<String> getInputFileNames() {
    String folderName =
        RateLimitingEdgeDecisionConverterTest.class
            .getClassLoader()
            .getResource(INPUT_DIR)
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(queriesFolder.listFiles())
        .map(file -> file.getName())
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
}
