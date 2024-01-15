package common

import java.sql.Date

case class Stocks(
                  company: String,
                  date: Date,
                  value: Double
                )