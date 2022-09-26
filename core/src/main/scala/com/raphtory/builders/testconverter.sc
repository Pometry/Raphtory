def converter[T](value: T): Long = value match {
  case _ => value.asInstanceOf[Long]
}

val longValue = converter[String]("1231233L")
println(longValue)