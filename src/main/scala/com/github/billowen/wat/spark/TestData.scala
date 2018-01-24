package com.github.billowen.wat.spark

import java.util.UUID

case class TestData(var project_id: UUID,
                    var lot_id: UUID,
                    var wafer_id: UUID,
                    var x: Int = -1,
                    var y: Int = -1,
                    var data_id: UUID = UUID.randomUUID(),
                    var measurement_id: Option[UUID] = None,
                    var value: Option[Double] = None,
                    var yield_type: String = "") {

}
