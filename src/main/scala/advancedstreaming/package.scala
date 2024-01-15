import org.apache.spark.sql.streaming.StreamingQuery

package object advancedstreaming {

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging
    /**
     * Create new Thread to loop from 1 to 100 and print query progress to console
     *
     * - Create new Thread with a lambda function
     * - If */
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString
        println(s"$i: $queryEventTime")
      }
    })
  }.start()

}
