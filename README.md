# bigcell_poc
Proof of concept BigCell

- A single spreadsheet
- No user authentication
- No locking 
- Operations : 
	- Load (source file name ) : will load the data from file (http url) 
	- Show (list columns, rows range) : will return a chunk of the unique spreadsheet 



-------------------------------------------------------------------------------------------------------
# Globale : 
-------------------------------------------------------------------------------------------------------
	-- Prepare Window spec to regenerate row_num :
	import org.apache.spark.sql.expressions.Window
	val rownumSpecs = Window.partitionBy(lit(1)).orderBy("row_num_sort_key")

-------------------------------------------------------------------------------------------------------
# Operation 1 : Create a new spreadsheet (from a csv file) :  
-------------------------------------------------------------------------------------------------------
## Notes :
1) keep the natural order (from the original file) using a Window operation combined with monotonically_increasing_id (Spark function)
2) We generate a row_num, that will be used in any row operation (deletion, update, insertion)
3) The names are reserved column names = row_num_partition_key, row_num_sort_key  , row_num|acc_row_num 

## Step 1 : Loading Spreadsheet from CSV :

	val sp_v0 = spark.read.option("header","true").csv("/data/flight-data/csv/*").cache

	+--------------------+-------------------+-----+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
	+--------------------+-------------------+-----+
	|       United States|            Romania|    1|
	|       United States|            Ireland|  264|
	|       United States|              India|   69|
	|               Egypt|      United States|   24|
	|   Equatorial Guinea|      United States|    1|
	|       United States|          Singapore|   25|
	|       United States|            Grenada|   54|
	|          Costa Rica|      United States|  477|
	|             Senegal|      United States|   29|
	|       United States|   Marshall Islands|   44|
	|              Guyana|      United States|   17|
	|       United States|       Sint Maarten|   53|
	|               Malta|      United States|    1|
	|             Bolivia|      United States|   46|
	|            Anguilla|      United States|   21|
	|Turks and Caicos ...|      United States|  136|
	|       United States|        Afghanistan|    2|
	|Saint Vincent and...|      United States|    1|
	|               Italy|      United States|  390|
	|       United States|             Russia|  156|
	+--------------------+-------------------+-----+

## Step 2 : Add row_num column 
	val sp_v1 = sp_v0.withColumn("row_num_sort_key", monotonically_increasing_id).
					  withColumn("row_num_natural_sort_key", 'row_num_sort_key).
					  withColumn("row_num", row_number.over(rownumSpecs))

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|
	|       United States|              India|   69|                    1|               2|                       2|      3|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|
	|       United States|   Marshall Islands|   44|                    1|               9|                       9|     10|
	|              Guyana|      United States|   17|                    1|              10|                      10|     11|
	|       United States|       Sint Maarten|   53|                    1|              11|                      11|     12|
	|               Malta|      United States|    1|                    1|              12|                      12|     13|
	|             Bolivia|      United States|   46|                    1|              13|                      13|     14|
	|            Anguilla|      United States|   21|                    1|              14|                      14|     15|
	|Turks and Caicos ...|      United States|  136|                    1|              15|                      15|     16|
	|       United States|        Afghanistan|    2|                    1|              16|                      16|     17|
	|Saint Vincent and...|      United States|    1|                    1|              17|                      17|     18|
	|               Italy|      United States|  390|                    1|              18|                      18|     19|
	|       United States|             Russia|  156|                    1|              19|                      19|     20|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+


-------------------------------------------------------------------------------------------------------
# Operation 2 : Remove a row  (remove row_num = 10)
-------------------------------------------------------------------------------------------------------

## Notes :
1) recompute row_num (by reapplying the window row_number function)
2) recompute all the other computed columns

## Step 1 : Create a new data frame without the given line 

	val sp_v2 = sp_v1.filter("row_num != 10")

	+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|             Romania|    1|                    1|               0|                       0|      1|
	|       United States|             Ireland|  264|                    1|               1|                       1|      2|
	|       United States|               India|   69|                    1|               2|                       2|      3|
	|               Egypt|       United States|   24|                    1|               3|                       3|      4|
	|   Equatorial Guinea|       United States|    1|                    1|               4|                       4|      5|
	|       United States|           Singapore|   25|                    1|               5|                       5|      6|
	|       United States|             Grenada|   54|                    1|               6|                       6|      7|
	|          Costa Rica|       United States|  477|                    1|               7|                       7|      8|
	|             Senegal|       United States|   29|                    1|               8|                       8|      9|
	|              Guyana|       United States|   17|                    1|              10|                      10|     11|
	|       United States|        Sint Maarten|   53|                    1|              11|                      11|     12|
	|               Malta|       United States|    1|                    1|              12|                      12|     13|
	|             Bolivia|       United States|   46|                    1|              13|                      13|     14|
	|            Anguilla|       United States|   21|                    1|              14|                      14|     15|
	|Turks and Caicos ...|       United States|  136|                    1|              15|                      15|     16|
	|       United States|         Afghanistan|    2|                    1|              16|                      16|     17|
	|Saint Vincent and...|       United States|    1|                    1|              17|                      17|     18|
	|               Italy|       United States|  390|                    1|              18|                      18|     19|
	|       United States|              Russia|  156|                    1|              19|                      19|     20|
	|       United States|Federated States ...|   48|                    1|              20|                      20|     21|
	+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+


## Step 2 : Recompute the row_num column 
	
	val sp_v3 = sp_v2.withColumn("row_num", row_number.over(rownumSpecs))

+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
|   DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
|       United States|             Romania|    1|                    1|               0|                       0|      1|
|       United States|             Ireland|  264|                    1|               1|                       1|      2|
|       United States|               India|   69|                    1|               2|                       2|      3|
|               Egypt|       United States|   24|                    1|               3|                       3|      4|
|   Equatorial Guinea|       United States|    1|                    1|               4|                       4|      5|
|       United States|           Singapore|   25|                    1|               5|                       5|      6|
|       United States|             Grenada|   54|                    1|               6|                       6|      7|
|          Costa Rica|       United States|  477|                    1|               7|                       7|      8|
|             Senegal|       United States|   29|                    1|               8|                       8|      9|
|              Guyana|       United States|   17|                    1|              10|                      10|     10|
|       United States|        Sint Maarten|   53|                    1|              11|                      11|     11|
|               Malta|       United States|    1|                    1|              12|                      12|     12|
|             Bolivia|       United States|   46|                    1|              13|                      13|     13|
|            Anguilla|       United States|   21|                    1|              14|                      14|     14|
|Turks and Caicos ...|       United States|  136|                    1|              15|                      15|     15|
|       United States|         Afghanistan|    2|                    1|              16|                      16|     16|
|Saint Vincent and...|       United States|    1|                    1|              17|                      17|     17|
|               Italy|       United States|  390|                    1|              18|                      18|     18|
|       United States|              Russia|  156|                    1|              19|                      19|     19|
|       United States|Federated States ...|   48|                    1|              20|                      20|     20|
+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+

## Step 3 : Recompute the computed columns
	
	// TODO 


-------------------------------------------------------------------------------------------------------
# Operation 3 : Add one row  (add empty after row_num = 10)
------------------------------------------------------------------------------------------------------- 

## Notes :
1) increment the row_num_partition_key starting from the row that comes after the new row
2) recompute row_num (by reapplying the window row_number function)
3) recompute all the other computed columns

## Step 1 : Increment  row_num_sort_key   for each row where row_num > 10

	val sp_v4 = sp_v3.
		withColumn("row_num_sort_key", when(expr("row_num > 10"), expr("row_num_sort_key  + 1")).otherwise('row_num_sort_key)).
		withColumn("row_num_natural_sort_key", when(expr("row_num > 10"), expr("row_num_natural_sort_key  + 1")).otherwise('row_num_natural_sort_key))
		

+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
|   DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+
|       United States|             Romania|    1|                    1|               0|                       0|      1|
|       United States|             Ireland|  264|                    1|               1|                       1|      2|
|       United States|               India|   69|                    1|               2|                       2|      3|
|               Egypt|       United States|   24|                    1|               3|                       3|      4|
|   Equatorial Guinea|       United States|    1|                    1|               4|                       4|      5|
|       United States|           Singapore|   25|                    1|               5|                       5|      6|
|       United States|             Grenada|   54|                    1|               6|                       6|      7|
|          Costa Rica|       United States|  477|                    1|               7|                       7|      8|
|             Senegal|       United States|   29|                    1|               8|                       8|      9|
|              Guyana|       United States|   17|                    1|              10|                      10|     10|
																														 <<< The new row will be inserted here
|       United States|        Sint Maarten|   53|                    1|              12|                      12|     11|
|               Malta|       United States|    1|                    1|              13|                      13|     12|
|             Bolivia|       United States|   46|                    1|              14|                      14|     13|
|            Anguilla|       United States|   21|                    1|              15|                      15|     14|
|Turks and Caicos ...|       United States|  136|                    1|              16|                      16|     15|
|       United States|         Afghanistan|    2|                    1|              17|                      17|     16|
|Saint Vincent and...|       United States|    1|                    1|              18|                      18|     17|
|               Italy|       United States|  390|                    1|              19|                      19|     18|
|       United States|              Russia|  156|                    1|              20|                      20|     19|
|       United States|Federated States ...|   48|                    1|              21|                      21|     20|
+--------------------+--------------------+-----+---------------------+----------------+------------------------+-------+



## Step 2 : Prepare and insert new row with : 


	val newRow = sp_v4.filter(expr("row_num == 10")).
					withColumn("DEST_COUNTRY_NAME", lit("United States")).
					withColumn("ORIGIN_COUNTRY_NAME", lit("Marshall Islands")).
					withColumn("count", lit(44)).
					withColumn("row_num_sort_key", expr("row_num_sort_key + 1")).
					withColumn("row_num_natural_sort_key", expr("row_num_natural_sort_key + 1"))						

	+-----------------+-------------------+-----+-----------------+------------------+-------+
	|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key  |row_num|
	+-----------------+-------------------+-----+-----------------+------------------+-------+
	|    United States|   Marshall Islands|   44|                1|                11|     10|
	+-----------------+-------------------+-----+-----------------+------------------+-------+



## Step 3 : Insert the new row in the main dataframe and recompute the row_num : 

	val sp_v5 = sp_v4.union(newRow).withColumn("row_num", row_number.over(rownumSpecs))

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|
	|       United States|              India|   69|                    1|               2|                       2|      3|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|<<< The new row inserted, and row_num updated
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|
	|               Malta|      United States|    1|                    1|              13|                      13|     13|
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|
	|               Italy|      United States|  390|                    1|              19|                      19|     19|
	|       United States|             Russia|  156|                    1|              20|                      20|     20|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+


## Step 4 : Recompute the computed columns
	
	// TODO 

-------------------------------------------------------------------------------------------------------
# Operation 4 : Add column - local operation (name : count_100, formula   : count >= 100)
-------------------------------------------------------------------------------------------------------

 	val sp_v6 = sp_v5.withColumn("count_100", expr("count >= 100"))

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|count_100|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|    false|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|     true|
	|       United States|              India|   69|                    1|               2|                       2|      3|    false|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|    false|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|    false|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|    false|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|    false|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|     true|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|    false|
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|    false|
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|    false|
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|    false|
	|               Malta|      United States|    1|                    1|              13|                      13|     13|    false|
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|    false|
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|    false|
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|     true|
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|    false|
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|    false|
	|               Italy|      United States|  390|                    1|              19|                      19|     19|     true|
	|       United States|             Russia|  156|                    1|              20|                      20|     20|     true|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+

-------------------------------------------------------------------------------------------------------
# Operation 5 : Remove a column (count_100)
-------------------------------------------------------------------------------------------------------
		
	val columns = sp_v6.columns.filter(x => x != "count_100").map(x => col(x))

	val sp_v7 = sp_v6.select(columns:_*)

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|
	|       United States|              India|   69|                    1|               2|                       2|      3|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|
	|               Malta|      United States|    1|                    1|              13|                      13|     13|
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|
	|               Italy|      United States|  390|                    1|              19|                      19|     19|
	|       United States|             Russia|  156|                    1|              20|                      20|     20|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+

-------------------------------------------------------------------------------------------------------
# Operation 6 : new column empty string column (name : "empty_col")
-------------------------------------------------------------------------------------------------------

	val sp_v8 = sp_v7.withColumn("empty_col",lit(""))
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|empty_col|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|         |
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|         |
	|       United States|              India|   69|                    1|               2|                       2|      3|         |
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|         |
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|         |
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|         |
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|         |
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|         |
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|         |
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|         |
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|         |
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|         |
	|               Malta|      United States|    1|                    1|              13|                      13|     13|         |
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|         |
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|         |
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|         |
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|         |
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|         |
	|               Italy|      United States|  390|                    1|              19|                      19|     19|         |
	|       United States|             Russia|  156|                    1|              20|                      20|     20|         |
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+---------+

-------------------------------------------------------------------------------------------------------
# Operation 7 : add new column computed from row_num (name : "is_even_row", formula : "MOD(row_num, 2) == 0" )
-------------------------------------------------------------------------------------------------------

	val sp_v9 = sp_v7.withColumn("is_even_row", expr("(row_num % 2) == 0"))

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|is_even_row|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|      false|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|       true|
	|       United States|              India|   69|                    1|               2|                       2|      3|      false|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|       true|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|      false|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|       true|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|      false|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|       true|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|      false|
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|       true|
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|      false|
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|       true|
	|               Malta|      United States|    1|                    1|              13|                      13|     13|      false|
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|       true|
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|      false|
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|       true|
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|      false|
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|       true|
	|               Italy|      United States|  390|                    1|              19|                      19|     19|      false|
	|       United States|             Russia|  156|                    1|              20|                      20|     20|       true|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+

-------------------------------------------------------------------------------------------------------
# Operation 8 : add new column computed from a windowed agregation from another column (name : "accu_row_num", formula : "sum(row_num[:0])")
-------------------------------------------------------------------------------------------------------

The formula sum(row_num[:0]) ==> means we will compute a sum agregate over the row_num column taking a window that goes from the 1st row to the current row included.

## Step 1: convert formula into Window agregation : 

	//formula in BigCell Formula Language sum(row_num[:-1]), will be translated into :
	val specs = Window.orderBy("row_num_sort_key").partitionBy("row_num_partition_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
	val formula = sum('row_num).over(specs)	

## Step 2 : apply the formula : 
	
	val sp_v10 = sp_v7.withColumn("acc_row_num", formula)
	
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|acc_row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|       United States|            Romania|    1|                    1|               0|                       0|      1|          1|
	|       United States|            Ireland|  264|                    1|               1|                       1|      2|          3|
	|       United States|              India|   69|                    1|               2|                       2|      3|          6|
	|               Egypt|      United States|   24|                    1|               3|                       3|      4|         10|
	|   Equatorial Guinea|      United States|    1|                    1|               4|                       4|      5|         15|
	|       United States|          Singapore|   25|                    1|               5|                       5|      6|         21|
	|       United States|            Grenada|   54|                    1|               6|                       6|      7|         28|
	|          Costa Rica|      United States|  477|                    1|               7|                       7|      8|         36|
	|             Senegal|      United States|   29|                    1|               8|                       8|      9|         45|
	|              Guyana|      United States|   17|                    1|              10|                      10|     10|         55|
	|       United States|   Marshall Islands|   44|                    1|              11|                      11|     11|         66|
	|       United States|       Sint Maarten|   53|                    1|              12|                      12|     12|         78|
	|               Malta|      United States|    1|                    1|              13|                      13|     13|         91|
	|             Bolivia|      United States|   46|                    1|              14|                      14|     14|        105|
	|            Anguilla|      United States|   21|                    1|              15|                      15|     15|        120|
	|Turks and Caicos ...|      United States|  136|                    1|              16|                      16|     16|        136|
	|       United States|        Afghanistan|    2|                    1|              17|                      17|     17|        153|
	|Saint Vincent and...|      United States|    1|                    1|              18|                      18|     18|        171|
	|               Italy|      United States|  390|                    1|              19|                      19|     19|        190|
	|       United States|             Russia|  156|                    1|              20|                      20|     20|        210|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+

-------------------------------------------------------------------------------------------------------
# Operation : sorting the spreadsheet ([{colum : 'DEST_COUNTRY_NAME', direction : 'desc'}, {colum : 'ORIGIN_COUNTRY_NAME', direction : 'desc}])
-------------------------------------------------------------------------------------------------------

## Step 1 : create a Window sorting spec 

	val spec = Window.partitionBy('row_num_partition_key).orderBy(desc("DEST_COUNTRY_NAME"), desc("ORIGIN_COUNTRY_NAME"))

## Step 2 : sort and recompute row_num and row_num_sort_key
	
	val sp_v11 = sp_v7.withColumn("row_num", row_number.over(spec)).withColumn("row_num_sort_key", monotonically_increasing_id)

	+-----------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+-----------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|         Zimbabwe|      United States|    2|                    1|    369367187456|              8589934717|      1|
	|           Zambia|      United States|    1|                    1|    369367187457|                     354|      2|
	|            Yemen|      United States|    1|                    1|    369367187458|                     554|      3|
	|          Vietnam|      United States|    1|                    1|    369367187459|                     203|      4|
	|          Vietnam|      United States|    1|                    1|    369367187460|                     715|      5|
	|          Vietnam|      United States|    2|                    1|    369367187461|              8589935039|      6|
	|        Venezuela|      United States|  377|                    1|    369367187462|                      58|      7|
	|        Venezuela|      United States|  290|                    1|    369367187463|                     314|      8|
	|        Venezuela|      United States|  373|                    1|    369367187464|                     573|      9|
	|        Venezuela|      United States|  350|                    1|    369367187465|              8589934653|     10|
	|        Venezuela|      United States|  389|                    1|    369367187466|              8589934899|     11|
	|        Venezuela|      United States|  335|                    1|    369367187467|              8589935145|     12|
	|          Uruguay|      United States|   54|                    1|    369367187468|                     247|     13|
	|          Uruguay|      United States|   43|                    1|    369367187469|                     508|     14|
	|          Uruguay|      United States|   50|                    1|    369367187470|                     759|     15|
	|          Uruguay|      United States|   57|                    1|    369367187471|              8589934836|     16|
	|          Uruguay|      United States|   53|                    1|    369367187472|              8589935080|     17|
	|          Uruguay|      United States|   60|                    1|    369367187473|              8589935322|     18|
	|    United States|           Zimbabwe|    2|                    1|    369367187474|              8589934679|     19|
	|    United States|            Vietnam|    1|                    1|    369367187475|                      76|     20|
	+-----------------+-------------------+-----+---------------------+----------------+------------------------+-------+

## Step 3 : recompute all the computed columns & apply the filters

	//TODO

-------------------------------------------------------------------------------------------------------
# Operation : cancel the sorting (return to the natural order)
-------------------------------------------------------------------------------------------------------

## Step 1 : create a Window sorting spec by row_num_natural_sort_key

	val spec = Window.partitionBy('row_num_partition_key).orderBy('row_num_natural_sort_key)

## Step 2 : sort and recompute row_num and row_num_sort_key
	
	val sp_v12 = sp_v11.withColumn("row_num", row_number.over(spec)).withColumn("row_num_sort_key", monotonically_increasing_id)

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|            Romania|    1|                    1|    369367187456|                       0|      1|
	|       United States|            Ireland|  264|                    1|    369367187457|                       1|      2|
	|       United States|              India|   69|                    1|    369367187458|                       2|      3|
	|               Egypt|      United States|   24|                    1|    369367187459|                       3|      4|
	|   Equatorial Guinea|      United States|    1|                    1|    369367187460|                       4|      5|
	|       United States|          Singapore|   25|                    1|    369367187461|                       5|      6|
	|       United States|            Grenada|   54|                    1|    369367187462|                       6|      7|
	|          Costa Rica|      United States|  477|                    1|    369367187463|                       7|      8|
	|             Senegal|      United States|   29|                    1|    369367187464|                       8|      9|
	|              Guyana|      United States|   17|                    1|    369367187465|                      10|     10|
	|       United States|   Marshall Islands|   44|                    1|    369367187466|                      11|     11|
	|       United States|       Sint Maarten|   53|                    1|    369367187467|                      12|     12|
	|               Malta|      United States|    1|                    1|    369367187468|                      13|     13|
	|             Bolivia|      United States|   46|                    1|    369367187469|                      14|     14|
	|            Anguilla|      United States|   21|                    1|    369367187470|                      15|     15|
	|Turks and Caicos ...|      United States|  136|                    1|    369367187471|                      16|     16|
	|       United States|        Afghanistan|    2|                    1|    369367187472|                      17|     17|
	|Saint Vincent and...|      United States|    1|                    1|    369367187473|                      18|     18|
	|               Italy|      United States|  390|                    1|    369367187474|                      19|     19|
	|       United States|             Russia|  156|                    1|    369367187475|                      20|     20|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+

## Step 3 : recompute all the computed columns

	//TODO

-------------------------------------------------------------------------------------------------------
# Operation : apply filter based on an expression (visual filtering) : filtering formula = "DEST_COUNTRY_NAME = 'United States'"
-------------------------------------------------------------------------------------------------------

## Step 1 : prepare the formula
	
	val formula = expr("DEST_COUNTRY_NAME = 'United States' ")
	
## Step 2 : sort and recompute row_num and row_num_sort_key
	
	val sp_v13 = sp_v12.withColumn("row_visible", formula)

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|row_visible|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|       United States|            Romania|    1|                    1|    369367187456|                       0|      1|       true|
	|       United States|            Ireland|  264|                    1|    369367187457|                       1|      2|       true|
	|       United States|              India|   69|                    1|    369367187458|                       2|      3|       true|
	|               Egypt|      United States|   24|                    1|    369367187459|                       3|      4|      false|
	|   Equatorial Guinea|      United States|    1|                    1|    369367187460|                       4|      5|      false|
	|       United States|          Singapore|   25|                    1|    369367187461|                       5|      6|       true|
	|       United States|            Grenada|   54|                    1|    369367187462|                       6|      7|       true|
	|          Costa Rica|      United States|  477|                    1|    369367187463|                       7|      8|      false|
	|             Senegal|      United States|   29|                    1|    369367187464|                       8|      9|      false|
	|              Guyana|      United States|   17|                    1|    369367187465|                      10|     10|      false|
	|       United States|   Marshall Islands|   44|                    1|    369367187466|                      11|     11|       true|
	|       United States|       Sint Maarten|   53|                    1|    369367187467|                      12|     12|       true|
	|               Malta|      United States|    1|                    1|    369367187468|                      13|     13|      false|
	|             Bolivia|      United States|   46|                    1|    369367187469|                      14|     14|      false|
	|            Anguilla|      United States|   21|                    1|    369367187470|                      15|     15|      false|
	|Turks and Caicos ...|      United States|  136|                    1|    369367187471|                      16|     16|      false|
	|       United States|        Afghanistan|    2|                    1|    369367187472|                      17|     17|       true|
	|Saint Vincent and...|      United States|    1|                    1|    369367187473|                      18|     18|      false|
	|               Italy|      United States|  390|                    1|    369367187474|                      19|     19|      false|
	|       United States|             Russia|  156|                    1|    369367187475|                      20|     20|       true|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+-----------+


## Step 3 : filter visualisation based on "row_visible" value :
	
	sp_v13.filter('row_visible).show

	+-----------------+--------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|row_visible|
	+-----------------+--------------------+-----+---------------------+----------------+------------------------+-------+-----------+
	|    United States|             Romania|    1|                    1|    369367187456|                       0|      1|       true|
	|    United States|             Ireland|  264|                    1|    369367187457|                       1|      2|       true|
	|    United States|               India|   69|                    1|    369367187458|                       2|      3|       true|
	|    United States|           Singapore|   25|                    1|    369367187461|                       5|      6|       true|
	|    United States|             Grenada|   54|                    1|    369367187462|                       6|      7|       true|
	|    United States|    Marshall Islands|   44|                    1|    369367187466|                      11|     11|       true|
	|    United States|        Sint Maarten|   53|                    1|    369367187467|                      12|     12|       true|
	|    United States|         Afghanistan|    2|                    1|    369367187472|                      17|     17|       true|
	|    United States|              Russia|  156|                    1|    369367187475|                      20|     20|       true|
	|    United States|Federated States ...|   48|                    1|    369367187476|                      21|     21|       true|
	|    United States|         Netherlands|  570|                    1|    369367187478|                      23|     23|       true|
	|    United States|             Senegal|   46|                    1|    369367187485|                      30|     30|       true|
	|    United States|              Angola|   18|                    1|    369367187487|                      32|     32|       true|
	|    United States|            Anguilla|   20|                    1|    369367187490|                      35|     35|       true|
	|    United States|             Ecuador|  345|                    1|    369367187495|                      40|     40|       true|
	|    United States|              Cyprus|    1|                    1|    369367187499|                      44|     44|       true|
	|    United States|Bosnia and Herzeg...|    1|                    1|    369367187501|                      46|     46|       true|
	|    United States|            Portugal|  104|                    1|    369367187502|                      47|     47|       true|
	|    United States|          Costa Rica|  501|                    1|    369367187503|                      48|     48|       true|
	|    United States|           Guatemala|  333|                    1|    369367187504|                      49|     49|       true|
	+-----------------+--------------------+-----+---------------------+----------------+------------------------+-------+-----------+


-------------------------------------------------------------------------------------------------------
# Operation : cancel filter = Opertation 5 (remove column where colum = 'row_visible')
-------------------------------------------------------------------------------------------------------

	val columns = sp_v13.columns.filter( x => x != "row_visible").map( x => col(x))

	val sp_v14 = sp_v13.select(columns:_*)

	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|row_num_partition_key|row_num_sort_key|row_num_natural_sort_key|row_num|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
	|       United States|            Romania|    1|                    1|    369367187456|                       0|      1|
	|       United States|            Ireland|  264|                    1|    369367187457|                       1|      2|
	|       United States|              India|   69|                    1|    369367187458|                       2|      3|
	|               Egypt|      United States|   24|                    1|    369367187459|                       3|      4|
	|   Equatorial Guinea|      United States|    1|                    1|    369367187460|                       4|      5|
	|       United States|          Singapore|   25|                    1|    369367187461|                       5|      6|
	|       United States|            Grenada|   54|                    1|    369367187462|                       6|      7|
	|          Costa Rica|      United States|  477|                    1|    369367187463|                       7|      8|
	|             Senegal|      United States|   29|                    1|    369367187464|                       8|      9|
	|              Guyana|      United States|   17|                    1|    369367187465|                      10|     10|
	|       United States|   Marshall Islands|   44|                    1|    369367187466|                      11|     11|
	|       United States|       Sint Maarten|   53|                    1|    369367187467|                      12|     12|
	|               Malta|      United States|    1|                    1|    369367187468|                      13|     13|
	|             Bolivia|      United States|   46|                    1|    369367187469|                      14|     14|
	|            Anguilla|      United States|   21|                    1|    369367187470|                      15|     15|
	|Turks and Caicos ...|      United States|  136|                    1|    369367187471|                      16|     16|
	|       United States|        Afghanistan|    2|                    1|    369367187472|                      17|     17|
	|Saint Vincent and...|      United States|    1|                    1|    369367187473|                      18|     18|
	|               Italy|      United States|  390|                    1|    369367187474|                      19|     19|
	|       United States|             Russia|  156|                    1|    369367187475|                      20|     20|
	+--------------------+-------------------+-----+---------------------+----------------+------------------------+-------+
