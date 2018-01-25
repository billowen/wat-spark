package com.github.billowen.wat.spark

import java.util.UUID

case class Wafer(project_id: UUID,
                 lot: String,
                 wafer: String,
                 var die_cnt: Long = 0,
                 var test_cnt: Long = 0,
                 var yield_rate: Double = 0.0) {

}
