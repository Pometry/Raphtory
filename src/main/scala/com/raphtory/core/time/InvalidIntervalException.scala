package com.raphtory.core.time

class InvalidIntervalException(interval: String, cause: Throwable)
        extends Exception(s"Invalid interval: '$interval'", cause)
