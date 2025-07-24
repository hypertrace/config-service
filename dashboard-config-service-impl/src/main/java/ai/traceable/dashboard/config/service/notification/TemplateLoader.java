package ai.traceable.dashboard.config.service.notification;

import com.google.common.collect.ImmutableMap;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;

@Singleton
public class TemplateLoader {
  private static final String TEMPLATE_FOLDER = "/templates";
  private static final Map<TemplateName, String> TEMPLATE_MAP =
      ImmutableMap.<TemplateName, String>builder()
          .put(TemplateName.DASHBOARD_INVITATION, "InvitationMail.ftl")
          .build();

  private final Map<TemplateName, String> templates;

  TemplateLoader() {
    this.templates = preloadTemplates();
  }

  String readTemplate(TemplateName template) {
    return templates.get(template);
  }

  @SneakyThrows
  private Map<TemplateName, String> preloadTemplates() {
    ImmutableMap.Builder<TemplateName, String> templatesBuilder = ImmutableMap.builder();

    for (Map.Entry<TemplateName, String> entry : TEMPLATE_MAP.entrySet()) {
      TemplateName templateName = entry.getKey();
      String templateFileName = entry.getValue();
      String templateContent = loadTemplateContent(templateFileName);
      templatesBuilder.put(templateName, templateContent);
    }

    return templatesBuilder.build();
  }

  @SneakyThrows
  private String loadTemplateContent(String templateFileName) {
    return new String(
        this.getClass()
            .getResourceAsStream(this.getTemplatePath(Objects.requireNonNull(templateFileName)))
            .readAllBytes());
  }

  private String getTemplatePath(String templateId) {
    return String.join("/", TEMPLATE_FOLDER, templateId);
  }
}
