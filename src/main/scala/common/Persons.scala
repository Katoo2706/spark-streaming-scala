package common

import java.sql.Date

case class Persons(
                   id: Int,
                   firstName: String,
                   middleName: String,
                   lastName: String,
                   gender: String,
                   birthDate: Date,
                   ssn: String,
                   salary: Int
                 )