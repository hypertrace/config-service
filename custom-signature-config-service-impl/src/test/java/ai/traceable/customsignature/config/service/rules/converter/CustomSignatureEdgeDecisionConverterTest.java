package ai.traceable.customsignature.config.service.rules.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.customsignature.config.service.rules.converter.expression.CustomSignatureExpressionConverter;
import ai.traceable.customsignature.config.service.rules.converter.expression.ExpressionConverterModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
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

class CustomSignatureEdgeDecisionConverterTest {

  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  static CustomSignatureEdgeDecisionConverter converter;

  @BeforeAll
  static void setup() {
    Injector injector = Guice.createInjector(new ExpressionConverterModule());
    Set<CustomSignatureExpressionConverter> conditionConverters =
        injector.getInstance(
            Key.get(new TypeLiteral<Set<CustomSignatureExpressionConverter>>() {}));
    converter = new CustomSignatureEdgeDecisionConverter(conditionConverters);
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testEvaluateRule(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    CustomSignatureRule.Builder builder = CustomSignatureRule.newBuilder();
    parser.merge(inputFileStr, builder);
    EdgeDecisionEngineConfig output = converter.convert(List.of(builder.build()));
    EdgeDecisionEngineConfig.Builder expectedOutputBuilder = EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);

    assertEquals(expectedOutputBuilder.build(), output);
  }

  static List<String> getInputFileNames() {
    String folderName =
        CustomSignatureEdgeDecisionConverterTest.class
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
