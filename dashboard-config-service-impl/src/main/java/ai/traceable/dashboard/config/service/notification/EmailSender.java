package ai.traceable.dashboard.config.service.notification;

import static ai.traceable.dashboard.config.service.notification.TemplateName.DASHBOARD_INVITATION;

import ai.traceable.dashboard.config.service.DashboardConfigServiceConfig;
import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.notification.message.api.v2.NotificationMessage;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class EmailSender {
  private final EventProducer<String, NotificationMessage> emailEventProducer;
  private final EmailNotificationMessageConverter emailNotificationMessageConverter;
  private final DashboardConfigServiceConfig dashboardConfigServiceConfig;
  private static final String PARAM_AUTHOR_NAME = "authorName";
  private static final String PARAM_BOARD_NAME = "boardName";
  private static final String PARAM_BOARD_DESCRIPTION = "boardDescription";
  private static final String PARAM_DASHBOARD_URL = "dashboardUrl";
  private static final String DASHBOARD_PATH = "dashboard/";
  private static final String EMAIL_SUBJECT_FORMAT = "%s has shared the \"%s\" dashboard with you.";
  private static final String SLASH = "/";

  public void sendEmails(
      RequestContext requestContext, List<String> userEmails, Dashboard dashboard) {
    if (userEmails.isEmpty()) {
      log.info("No email addresses provided for dashboard sharing.");
      return;
    }
    try {
      String senderEmail = requestContext.getEmail().orElseThrow();
      String subject = buildSubject(senderEmail, dashboard.getName());
      Map<String, String> dashboardDetailsParams =
          buildDashboardDetailsParams(senderEmail, dashboard);
      kafkaBasedSender(
          new TemplateEmailRequest(
              userEmails, subject, dashboardDetailsParams, DASHBOARD_INVITATION));

    } catch (Exception e) {
      log.error(
          "Failed to prepare or send dashboard sharing email for dashboard: {}",
          dashboard.getId(),
          e);
    }
  }

  private Map<String, String> buildDashboardDetailsParams(String senderEmail, Dashboard dashboard) {
    Map<String, String> params = new HashMap<>();
    params.put(PARAM_AUTHOR_NAME, senderEmail);
    params.put(PARAM_BOARD_NAME, dashboard.getName());
    params.put(
        PARAM_BOARD_DESCRIPTION, dashboard.hasDescription() ? dashboard.getDescription() : "");
    String dashboardUrl = buildDashboardUrl(dashboard.getId());
    params.put(PARAM_DASHBOARD_URL, dashboardUrl);

    return params;
  }

  private String buildDashboardUrl(String dashboardId) {
    String baseUrl = dashboardConfigServiceConfig.getTraceableUrl();
    if (!baseUrl.endsWith(SLASH)) {
      baseUrl += SLASH;
    }
    return baseUrl + DASHBOARD_PATH + dashboardId;
  }

  private String buildSubject(String senderEmail, String dashboardName) {
    return String.format(EMAIL_SUBJECT_FORMAT, senderEmail, dashboardName);
  }

  public void kafkaBasedSender(TemplateEmailRequest request) {
    try {
      emailEventProducer.send(
          this.generateRandomKey(), this.emailNotificationMessageConverter.convertFrom(request));
    } catch (Exception e) {
      log.warn("Unable to send email, request {}", request, e);
    }
  }

  private String generateRandomKey() {
    return UUID.randomUUID().toString();
  }
}
