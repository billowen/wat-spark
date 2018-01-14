package com.github.billowen.wat.spark

import java.util.UUID

case class TestItem(var project_id: UUID,
                    var test_id : UUID = UUID.randomUUID(),
                    var name: String = "",
                    var measure_type: String = "",
                    var dut_id: Option[UUID] = None,
                    var formula: String = "",
                    var unit: String = "",
                    var target: Option[Double] = None,
                    var soft_low: Option[Double] = None,
                    var soft_high: Option[Double] = None,
                    var hard_low: Option[Double] = None,
                    var hard_high: Option[Double] = None) {

}
