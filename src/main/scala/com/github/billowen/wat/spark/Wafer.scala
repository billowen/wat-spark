package com.github.billowen.wat.spark

import java.util.UUID

case class Wafer(var project_id: UUID,
                 var lot_id: UUID,
                 var wafer_id: UUID = UUID.randomUUID(),
                 var wafer_name: String = "",
                 var die_cnt: Long = 0,
                 var test_cnt: Long = 0,
                 var yield_rate: Double = 0.0) {

}
