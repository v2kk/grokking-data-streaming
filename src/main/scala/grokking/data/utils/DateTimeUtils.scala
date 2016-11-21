package grokking.data.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeUtils {

    def formatDateTime(milis: Long): String = {
        
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.format(new Date(milis))
    }
    
    def format(milis: Long, pattern: String): String = {

        val format = new SimpleDateFormat(pattern)
        format.format(new Date(milis))
    }
    
    def format(timestamp: String, pattern: String): String = {

        val milis = timestamp.toLong
        val format = new SimpleDateFormat(pattern)
        format.format(new Date(milis))
    }
    
    def main(args: Array[String]) {
        
        val string = format(1479479682805L, "yyyyMMddHHmm");
        println(string)
    }
}