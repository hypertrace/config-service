package ai.traceable.dashboard.config.service.notification;

import ai.traceable.notification.message.api.v2.Email;
import ai.traceable.notification.message.api.v2.Notification;
import ai.traceable.notification.message.api.v2.NotificationMessage;
import ai.traceable.notification.message.api.v2.Text;
import jakarta.inject.Inject;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class EmailNotificationMessageConverter {
  TemplateLoader templateLoader;

  NotificationMessage convertFrom(TemplateEmailRequest templateEmailRequest) {
    return NotificationMessage.newBuilder()
        .putAllParamMap(templateEmailRequest.getParams())
        .setNotification(
            Notification.newBuilder()
                .setEmail(
                    Email.newBuilder()
                        .addAllToAddresses(
                            templateEmailRequest.getEmails().stream()
                                .map(this::buildAsLiteral)
                                .collect(Collectors.toUnmodifiableList()))
                        .setSubject(this.buildAsLiteral(templateEmailRequest.getSubject()))
                        .setBody(
                            Text.newBuilder()
                                .setTemplate(
                                    this.templateLoader.readTemplate(
                                        templateEmailRequest.getTemplateName())))))
        .build();
  }

  private Text buildAsLiteral(String literalText) {
    return Text.newBuilder().setLiteral(literalText).build();
  }
}
