package com.github.billowen.wat.spark

import java.util.UUID

case class Lot(var project_id: UUID,
               var lot_id: UUID = UUID.randomUUID(),
               var lot_name: String = "",
               var wafer_cnt: Long = 0,
               var yield_rate: Double = 0.0) {

}
