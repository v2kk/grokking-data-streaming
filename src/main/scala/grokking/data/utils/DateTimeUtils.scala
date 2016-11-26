package grokking.data.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.time.ZoneId
import java.util.Locale

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

    def takeHourBefore(currentMilis: Long, num: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHH")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("GMT0"));
        var start = Instant.ofEpochMilli(currentMilis)
        var results = List[String]()
        
        results ::= formatter.format(start)
        for(i <- 1 to num){
            start = start.minusSeconds(3600)
            results ::= formatter.format(start)
        }
        results
    }
    
    def takeMinuteBefore(currentMilis: Long, num: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("GMT0"));
        var start = Instant.ofEpochMilli(currentMilis)
        var results = List[String]()
        
        results ::= formatter.format(start)
        for(i <- 1 to num){
            start = start.minusSeconds(60)
            results ::= formatter.format(start)
        }
        results
    }
    
    def takeMinute(currentMilis: Long, num: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("GMT0"));
        var start = Instant.ofEpochMilli(currentMilis)
        start = start.minusSeconds(num * 3600)    // minus num hour
        val lastHour = formatter.format(start.minusSeconds((num -1) * 3600))
        println(lastHour)
        var results = List[String]()
        results ::= formatter.format(start)
        for(i <- 1 to num){
            start = start.minusSeconds(60)
            results ::= formatter.format(start)
        }
        results
    }

    def main(args: Array[String]) {

        val string = format(1451606400000L, "yyyyMMddHHmm");
        println(takeHourBefore(1451606400000L, 5))
        println(takeMinuteBefore(1451606400000L, 5))
        println(takeMinute(1451606400000L, 5))
    }
}