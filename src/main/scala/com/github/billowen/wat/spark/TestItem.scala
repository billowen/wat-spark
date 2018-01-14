package com.github.billowen.wat.spark

import java.util.UUID

case class TestItem(project_id: UUID) {
  var name = ""
  var measure_type = ""
  var test_id = UUID.randomUUID()
  var dut_id: UUID = null
  var formula = ""
  var unit = ""
  var target : Double = null
  var soft_low : Double = null
  var hard_low : Double = null
  var hard_high : Double = null
  var soft_high : Double = null
}
