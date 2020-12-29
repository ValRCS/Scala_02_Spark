package com.github.valrcs.spark

object MyUtil {
  def calcValue(inVal: Int) = {
    if (inVal == 0) 10.0 else inVal*2.5
  }
}
