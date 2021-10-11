package org.hypertrace.config.service.change.event.api;

import com.google.protobuf.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** The interface config change event generator. */
public interface ConfigChangeEventGenerator {

  /**
   * Send create notification for newly added config
   *
   * @param requestContext requestContext
   * @param configType config type
   * @param config newly created config
   */
  void sendCreateNotification(RequestContext requestContext, String configType, Value config);

  /**
   * Send delete notification for deleted config
   *
   * @param requestContext tenant Id
   * @param configType config type
   * @param config deleted config
   */
  void sendDeleteNotification(RequestContext requestContext, String configType, Value config);

  /**
   * Send update notification for updated config
   *
   * @param requestContext tenant Id
   * @param configType config type
   * @param prevConfig previous config
   * @param latestConfig latest config
   */
  void sendUpdateNotification(
      RequestContext requestContext, String configType, Value prevConfig, Value latestConfig);

  /**
   * Send create notification for newly added config
   *
   * @param requestContext requestContext
   * @param configType config type
   * @param context context
   * @param config newly created config
   */
  void sendCreateNotification(
      RequestContext requestContext, String configType, String context, Value config);

  /**
   * Send delete notification for deleted config
   *
   * @param requestContext tenant Id
   * @param configType config type
   * @param context context
   * @param config deleted config
   */
  void sendDeleteNotification(
      RequestContext requestContext, String configType, String context, Value config);

  /**
   * Send update notification for updated config
   *
   * @param requestContext tenant Id
   * @param configType config type
   * @param context context
   * @param prevConfig previous config
   * @param latestConfig latest config
   */
  void sendUpdateNotification(
      RequestContext requestContext,
      String configType,
      String context,
      Value prevConfig,
      Value latestConfig);
}
