package agh.wggios.analizadanych.transformation

import agh.wggios.analizadanych.caseclass.FlightCC
import agh.wggios.analizadanych.session.sparksession

class transformation extends sparksession{
  def USflights(flight_row: FlightCC): Boolean = {
    flight_row.DEST_COUNTRY_NAME == "United States" || flight_row.ORIGIN_COUNTRY_NAME == "United States"
  }
}
