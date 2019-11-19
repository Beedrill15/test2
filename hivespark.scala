spark.sql("use enhanceit").show
spark.sql("select count(*) from managers union select count(*) from engineers").show
