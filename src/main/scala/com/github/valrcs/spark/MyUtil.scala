package com.github.valrcs.spark

object MyUtil {
  def calcValue(inVal: Int) = {
    if (inVal == 0) 10.0 else inVal*2.5
  }

//  def calcCircleArea
  def calcCircleArea(inVal: Int):Double = {
    inVal*inVal*math.Pi
  }
}
