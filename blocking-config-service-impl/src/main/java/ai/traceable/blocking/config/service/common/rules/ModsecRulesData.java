package ai.traceable.blocking.config.service.common.rules;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class ModsecRulesData<T> {
  String modsecDirectivesBlob;
  String modsecRulesBlob;
  List<T> rules;

  public ModsecRulesData(
      String modsecDirectivesBlob,
      String modsecRulesBlob,
      List<String> ruleIdsToInclude,
      Collection<T> rules,
      Function<T, String> idExtractor) {
    this.modsecDirectivesBlob = modsecDirectivesBlob;
    this.modsecRulesBlob = modsecRulesBlob;
    this.rules =
        rules.stream()
            .filter(rule -> ruleIdsToInclude.contains(idExtractor.apply(rule)))
            .collect(Collectors.toUnmodifiableList());
  }

  public ModsecRulesData() {
    modsecDirectivesBlob = "";
    modsecRulesBlob = "";
    rules = Collections.emptyList();
  }
}
