package com.github.billowen.wat.spark

import java.util.UUID

case class Dut(project_id: UUID, name: String, module: String, design_type: String, attributes: Map[String, String]) {
  var dut_id = UUID.randomUUID()
}
