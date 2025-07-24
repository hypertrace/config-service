package ai.traceable.dashboard.config.service.notification;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

@AllArgsConstructor
@NonFinal
@Value
public class TemplateEmailRequest {
  List<String> emails;
  String subject;
  Map<String, String> params;
  TemplateName templateName;
}
