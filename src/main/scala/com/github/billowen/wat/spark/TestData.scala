package com.github.billowen.wat.spark

import java.util.UUID

case class TestData(project_id: UUID,
                    lot: String,
                    wafer: String,
                    x: Int,
                    y: Int,
                    measurement: String,
                    var value: Option[Double] = None,
                    var yield_type: Option[String] = None) {

}
