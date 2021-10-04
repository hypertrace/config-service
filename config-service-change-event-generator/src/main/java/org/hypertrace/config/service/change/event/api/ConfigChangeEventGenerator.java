package org.hypertrace.config.service.change.event.api;

import com.google.protobuf.Value;

/** The interface config change event generator. */
public interface ConfigChangeEventGenerator {

  /**
   * Send create notification for newly added config
   *
   * @param tenantId tenant Id
   * @param resourceName resource name
   * @param resourceNamespace resource namespace
   * @param context context
   * @param config newly created config
   */
  void sendCreateNotification(
      String tenantId, String resourceName, String resourceNamespace, String context, Value config);

  /**
   * Send delete notification for deleted config
   *
   * @param tenantId tenant Id
   * @param resourceName resource name
   * @param resourceNamespace resource namespace
   * @param context context
   * @param config deleted config
   */
  void sendDeleteNotification(
      String tenantId, String resourceName, String resourceNamespace, String context, Value config);

  /**
   * Send update notification for updated config
   *
   * @param tenantId tenant Id
   * @param resourceName resource name
   * @param resourceNamespace resource namespace
   * @param context context
   * @param prevConfig previous config
   * @param latestConfig latest config
   */
  void sendUpdateNotification(
      String tenantId,
      String resourceName,
      String resourceNamespace,
      String context,
      Value prevConfig,
      Value latestConfig);
}
