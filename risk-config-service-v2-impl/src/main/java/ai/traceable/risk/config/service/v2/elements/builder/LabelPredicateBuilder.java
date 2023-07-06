package ai.traceable.risk.config.service.v2.elements.builder;

import static ai.traceable.risk.config.service.v2.StringOperator.STRING_OPERATOR_EQUALS;

import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.StringPredicate;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.config.service.v1.Label;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class LabelPredicateBuilder {

  private final LabelsConfigProvider labelsConfigProvider;

  public Collection<RiskElementConfig> buildRiskElementConfigsWithPredicates(
      Collection<RiskElementConfig> riskElementConfigs) {
    List<Label> labels = labelsConfigProvider.getLabels(RequestContext.CURRENT.get());
    return riskElementConfigs.stream()
        .map(riskElementConfig -> addElementPredicate(riskElementConfig, labels))
        .collect(Collectors.toUnmodifiableList());
  }

  private RiskElementConfig addElementPredicate(
      RiskElementConfig riskElementConfig, List<Label> labels) {
    if (riskElementConfig.hasRiskElementPredicate()) {
      return riskElementConfig;
    }
    String labelId = riskElementConfig.getId();
    return riskElementConfig.toBuilder()
        .setRiskElementPredicate(buildElementPredicate(labelId, labels))
        .build();
  }

  private RiskElementPredicate buildElementPredicate(String labelId, List<Label> labels) {
    Optional<Label> matchedLabelOptional =
        labels.stream().filter(label -> labelId.equals(label.getId())).findFirst();
    if (matchedLabelOptional.isEmpty()) {
      throw Status.NOT_FOUND
          .withDescription(String.format("No label found for labelElementId:%s NOT FOUND", labelId))
          .asRuntimeException();
    }
    return RiskElementPredicate.newBuilder()
        .setLabelId(
            StringPredicate.newBuilder().setOperator(STRING_OPERATOR_EQUALS).setValue(labelId))
        .build();
  }
}
