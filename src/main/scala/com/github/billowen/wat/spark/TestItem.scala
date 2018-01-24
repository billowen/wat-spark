package com.github.billowen.wat.spark

import java.util.UUID

case class TestItem(project_id: UUID,
                    item: String,
                    var measurement: String = "",
                    var dut_name: String = "",
                    var formula: String = "",
                    var unit: String = "",
                    var target: Option[Double] = None,
                    var soft_low: Option[Double] = None,
                    var soft_high: Option[Double] = None,
                    var hard_low: Option[Double] = None,
                    var hard_high: Option[Double] = None) {

}
