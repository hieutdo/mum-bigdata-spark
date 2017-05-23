package my.org

import java.util.Date

case class LogEntry(host: String,
                    timestamp: Date,
                    method: String,
                    path: String,
                    protocol: String,
                    status: Int,
                    contentSize: Long)