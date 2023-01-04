package com.test

/**
 * These classes are in com.test to force the DynamicClassLoader to ship them over to the remote Raphtory cluster
 * otherwise the serialisation will fail
 *
 * any classes that are used as part of custom algorithms or even custom algos that are not part of core raphtory
 * need to be placed in this package to ensure they are serialised and sent over
 */
package object raphtory {}
