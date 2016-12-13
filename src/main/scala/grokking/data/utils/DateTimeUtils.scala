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

    /**
     * Return list [num] hour before current hour inclusive
     */
    def takeListHourBefore(currentMilis: Long, num: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHH")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
        var start = Instant.ofEpochMilli(currentMilis)
        var results = List[String]()
        
        results ::= formatter.format(start)
        for(i <- 1 to num - 1){
            start = start.minusSeconds(3600)
            results ::= formatter.format(start)
        }
        results
    }
    
    def takeListMinuteBefore(currentMilis: Long, num: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
        var start = Instant.ofEpochMilli(currentMilis)
        var results = List[String]()
        
        results ::= formatter.format(start)
        for(i <- 1 to num - 1){
            start = start.minusSeconds(60)
            results ::= formatter.format(start)
        }
        results
    }
    
    /**
     * Return list minute at [num] hour ago
     */
    def takeListMinute(currentMilis: Long, hourAgo: Int): List[String] = {

        val formatter =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                .withLocale(Locale.UK)
                .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
        var results = List[String]()
        var start = Instant.ofEpochMilli(currentMilis)
        start = start.minusSeconds(hourAgo * 3600)
        var lastHour = formatter.format(start)
        results ::= lastHour
        
        while( lastHour.slice(10, 12) != "59"){
            
            start = start.plusSeconds(60)
            lastHour = formatter.format(start)
            results ::= formatter.format(start)
        }

        results
    }

    def main(args: Array[String]) {

        val time = System.currentTimeMillis()
        println(takeListHourBefore(time, 5))
        println(takeListMinute(time, 5))
    }
}