package com.github.valrcs.spark

case class MyRow(id: Int = 1, description: String ="blank desc", weight: Double=1.0, value: Double=3.3)
//remember you want to avoid using mutable var in case class but it is possible, default is val
