package com.github.billowen.wat.spark

import java.util.UUID

case class Dut(var project_id: UUID,
               var dut_id: UUID = UUID.randomUUID(),
               var name: String = "",
               var module: String = "",
               var design_type: String = "",
               var attributes: Map[String, String] = Map()) {
}
