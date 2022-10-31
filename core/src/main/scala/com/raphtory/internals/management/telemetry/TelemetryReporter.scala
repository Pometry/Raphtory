package com.raphtory.internals.management.telemetry

trait TelemetryReporter {
  protected val telemetry: ComponentTelemetryHandler.type = ComponentTelemetryHandler
}
