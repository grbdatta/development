package com.test
import java.util._
import com.validity.OpenStreetMapUtils

object CountryValidation {

  def main(args: Array[String]) {
    
        
        val coords = OpenStreetMapUtils.getInstance().getCoordinates("BTM");
        System.out.println("City name is "+ coords);
    
  }
}