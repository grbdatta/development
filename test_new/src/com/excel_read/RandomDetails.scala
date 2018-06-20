package com.excel_read

object RandomDetails {

  def generateInfo(): List[List[Any]] = {
    
    val name_list = scala.util.Random.shuffle(List("Gourab Datta", "Nayan Saha", "Suneet kaur", "Biman Mandal", "Koushik Gupta", "Rajni Gupta", "Sayan Sen", "Somojit Kumar", "Amit bhatt","Manish Roy","Sayani Biswas","Subhasish Guha","Priyanshu Kumar"))
    val performance_list = scala.util.Random.shuffle(List(56, 34, 78, 89, 12, 43, 55, 40, 39,55,85,18,49))
    val city_list = scala.util.Random.shuffle(List("Kolkata", "Bangalore", "Chennai", "Delhi", "Nagpur", "Hyderabad", "Pune", "Nasik", "Jammu","Tripura","Mumbai","Agra","Vizag"))
    val profit_list = scala.util.Random.shuffle(List(45, 92, 63, 83, 15, 72, 44, 65, 82,78,42,9,39))

    val master_list = List(name_list, performance_list, city_list, profit_list)
    master_list

  }

}