package com.github.billowen.wat.spark

import java.util.UUID

case class Lot(project_id: UUID,
               lot: String,
               var wafer_cnt: Long = 0,
               var yield_rate: Double = 0.0) {

}
