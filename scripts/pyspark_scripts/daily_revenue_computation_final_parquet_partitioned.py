from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,round


spark = SparkSession. \
            builder. \
            appName("Compute Daily Revenue").\
            master('yarn').\
            getOrCreate()
#orders data frame-bronze            
orders=spark.\
    read.\
        csv("/user/sovik/retail_db/orders",\
            schema='''
            order_id INT,order_date STRING,order_customer_id INT,order_status STRING
            '''
            )

# Extract order_month and filter orders
orders = orders.withColumn('order_month', substr(col('order_date'), 1, 7))
    
#order-items data frame-bronze        
order_items=spark.\
    read.\
        csv("/user/sovik/retail_db/order_items",\
            schema='''
            order_item_id INT,
            order_item_order_id INT,
            order_item_product_id INT,
            order_item_quantity INT,
            order_item_subtotal FLOAT,
            order_item_product_price FLOAT
            '''
            )
#order-daily data-frame silver
orders_daily=orders.filter('''order_status IN ('COMPLETE','CLOSED')''').\
    join(order_items,orders['order_id']==order_items['order_item_id']).\
        groupBy('order_date').\
            agg(round(sum('order_item_subtotal'),2).alias('revenue')).\
                orderBy('order_date')
                
                

#daily revenue data-frame silver       
daily_revenue_partitioned=orders_daily.\
    toDF('order_date','order_revenue').\
    write.\
        mode('overwrite').\
            partitionBy('order_month').\
            parquet('/user/sovik/retail_db/daily_revenue_parquet')

#validate           
daily_revenue_partitioned.show()
daily_revenue_partitioned.count()            
