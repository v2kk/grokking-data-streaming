package grokking.data.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeUtils {

    def formatDateTime(milis: Long): String = {
        
        var date = ""
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        date = format.format(new Date(milis))
        date
    }
}