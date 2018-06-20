package com.excel_read

import org.apache.spark.SparkConf     
import org.apache.spark.streaming.StreamingContext        
import org.apache.spark.streaming.Seconds        
import twitter4j.conf.ConfigurationBuilder    
import twitter4j.auth.OAuthAuthorization    
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
 
object TwitterData {    
  def main(args: Array[String]) {    
 
    if (args.length < 4) {    
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +    
        "[<filters>]")
      System.exit(1)   
    }
    println(args(0))
 
    val appName = "TwitterData"    
    val conf = new SparkConf()    
    System.setProperty("hadoop.home.dir", "C:\\hadoop");
    conf.setAppName(appName).setMaster("local[3]")    
 
    val ssc = new StreamingContext(conf, Seconds(5))    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)    
    val filters = args.takeRight(args.length - 4)    
    val cb = new ConfigurationBuilder    
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)    
      .setOAuthConsumerSecret(consumerSecret)    
      .setOAuthAccessToken(accessToken)    
      .setOAuthAccessTokenSecret(accessTokenSecret)
      System.setProperty("https.protocols", "TLSv1.1");
      //.setHttpProxyHost("inblrwcg01.in.kworld.kpmg.com")
      //.setHttpProxyPort(8080);
      
    val auth = new OAuthAuthorization(cb.build)    
    val tweets = TwitterUtils.createStream(ssc, Some(auth))    
    val englishTweets = tweets.filter(word => word.getText().toString().contains("India"))
    /*englishTweets.foreachRDD
    {
      l=>
    }*/
    englishTweets.saveAsTextFiles("tweets", "json")
    ssc.start()    
    ssc.awaitTermination()    
 
  }  
}