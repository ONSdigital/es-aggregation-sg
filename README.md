# es-aggregattio-sg

## Wranglers

### Combiner
The combiner is used to join the outputs from the 3 aggregations back onto the original
 data. It is assumed that the imputed(or original if it didnt need imputing) data is 
 stored in an s3 bucket by the imputation module; and that each of the 3 aggregation 
 processes each write their output to sqs.<br><br>
 The combiner merely picks up the imputation data from s3, then 3 messages from the sqs
  queue. It joins these all together and sends onwards. The result of which is that the
   next module(disclosure) has the granular input data with the addition of aggregations 
   merged on.