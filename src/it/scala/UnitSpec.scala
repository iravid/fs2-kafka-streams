package com.iravid.fs2.kafka

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest._

abstract class UnitSpec
    extends WordSpec with Matchers with GeneratorDrivenPropertyChecks with OptionValues
