package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ServiceScopedInfo {
  List<BlockingDetails> blockingDetails = Collections.emptyList();
  List<ExclusionRule> exclusionRules = Collections.emptyList();
}
