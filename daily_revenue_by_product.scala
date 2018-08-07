// Get Daily Revenue by product considering completed and closed orders
// Data need to be sorted by ascending order by date and then descending order by revenue computed for each product for each Day
// Broadcast products and perform lookup into the broadcasted hash map
// Get the number of completed and closed orders when data is being filtered

// Order and order_items available in HDFS 
// /user/cxm350/spark_data/retail_db/orders	
// /user/cxm350/spark_data/retail_db/order_items

// Products available in Local

// /export/home/cxm350/spark_data/retail_db/products/part-00000

val orders_raw = sc.textFile("/user/cxm350/spark_data/retail_db/orders")
val order_items_raw = sc.textFile("/user/cxm350/spark_data/retail_db/order_items")
import scala.io.Source
val products_raw = Source.fromFile("/export/home/cxm350/spark_data/retail_db/products/part-00000").getLines().toList
val products = products_raw.map(rec => (rec.split(",")(0).toInt, rec.split(",")(2))).toMap
val pr_bv = sc.broadcast(products)

// Get the count while filtering COMPLETE and CLOSED orders
val completed_order_cnt = sc.accumulator(0, "Completed/Closed Order Count")
val non_complete_order_cnt = sc.accumulator(0, "Non Complete Order Count")

val order_filtered=orders_raw.
filter(rec => {
val completedOrder = (rec.split(",")(3)== "COMPLETE" || rec.split(",")(3)=="CLOSED")
if(completedOrder) completed_order_cnt += 1
else  non_complete_order_cnt += 1
completedOrder  
}).
map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))

val order_items = order_items_raw.map(rec => (rec.split(",")(1).toInt,(rec.split(",")(2).toInt,rec.split(",")(4).toFloat)))

order_filtered.join(order_items).
map(rec => ((rec._2._2._1, rec._2._1),rec._2._2._2)).
reduceByKey((total, value) => total + value).
map(rec=> ((rec._1._2, -rec._2), rec._1._1)).
sortByKey().
map(rec => (rec._1._1+","+pr_bv.value(rec._2)+","+ -rec._1._2).toString).
saveAsTextFile("/user/cxm350/spark_data/retail_db/revenueOutput1")
