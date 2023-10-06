package ai.traceable.modsecurity.rule.conversion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter;
import ai.traceable.modsecurity.rule.conversion.clause.ModsecVariableConverter;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import com.google.common.io.Resources;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;

public class ModsecRuleConverterTest {

  private final ModsecVariableConverter variableConverter = new ModsecVariableConverter();
  private final ModsecOperatorConverter operatorConverter = new ModsecOperatorConverter();
  private ModsecRuleConverter modsecRuleConverter =
      new ModsecRuleConverterImpl(
          new CustomModsecValueMatchClauseConverter(variableConverter, operatorConverter),
          new CustomModsecKeyValueMatchClauseConverter(variableConverter, operatorConverter));

  @Test
  public void testConvertedModsecRules() throws Exception {
    List<CustomModsecRule> customModsecRules = new ArrayList<>();
    customModsecRules.addAll(ModsecValueMatchConverterTest.getSampleCustomModsecRules());
    customModsecRules.addAll(ModsecKeyValueMatchConverterTest.getSampleCustomModsecRules());

    String fileRulesBlob =
        Resources.toString(
            this.getClass()
                .getClassLoader()
                .getResource("conversion/sample-custom-modsec-rules.conf"),
            StandardCharsets.UTF_8);
    customModsecRules.sort(Comparator.comparing(CustomModsecRule::toString));
    String convertedModsecRulesBlob = modsecRuleConverter.getModsecRulesBlob(customModsecRules);

    assertEquals(fileRulesBlob, convertedModsecRulesBlob);
    if (SystemUtils.IS_OS_LINUX) {
      assertEquals(Status.OK, ModsecRuleEngineUtils.validate(convertedModsecRulesBlob));
    }
  }
}
