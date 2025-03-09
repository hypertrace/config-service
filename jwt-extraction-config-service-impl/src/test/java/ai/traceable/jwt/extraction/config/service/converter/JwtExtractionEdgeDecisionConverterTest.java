package ai.traceable.jwt.extraction.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class JwtExtractionEdgeDecisionConverterTest {
  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  static JwtExtractionEdgeDecisionConverter converter;

  @BeforeAll
  static void setup() {
    converter = new JwtExtractionEdgeDecisionConverter();
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testConvertRule(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    JwtExtractionRule.Builder jwtExtractionRuleBuilder = JwtExtractionRule.newBuilder();
    parser.merge(inputFileStr, jwtExtractionRuleBuilder);
    JwtExtractionRule jwtExtractionRule = jwtExtractionRuleBuilder.build();
    EdgeDecisionEngineConfig actualOutput = converter.convert(List.of(jwtExtractionRule));
    EdgeDecisionEngineConfig.Builder expectedOutputBuilder = EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);
    EdgeDecisionEngineConfig expectedOutput = expectedOutputBuilder.build();

    assertEquals(expectedOutput, actualOutput);
  }

  static List<String> getInputFileNames() {
    String folderName =
        JwtExtractionEdgeDecisionConverterTest.class
            .getClassLoader()
            .getResource(INPUT_DIR)
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(queriesFolder.listFiles())
        .map(file -> file.getName())
        .sorted()
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
