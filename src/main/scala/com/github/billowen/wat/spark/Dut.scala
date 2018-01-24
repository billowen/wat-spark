package com.github.billowen.wat.spark

import java.util.UUID

case class Dut(project_id: UUID,
               dut_name: String,
               var module: String = "",
               var design_type: String = "",
               var attributes: Map[String, String] = Map()) {
}
