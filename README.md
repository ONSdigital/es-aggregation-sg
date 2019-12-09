
# es-aggregation-sg

## Wranglers

### Calculate Column (EntRef/County/..) Wrangler

The wrangler is responsible for preparing the data, invoking the lambda and then sending the data downstream along with the respective notification messages (SNS).

Steps performed:

    - Retrieves data From S3 bucket
    - Filters the data by current period
    - Invokes method lambda
    - Puts the aggregated data onto the SQS queue
    - Sends SNS message
 <hr>
 
### Calculate Top 2 Wrangler

The wrangler is responsible for preparing the data, invoking the method lambda and sending the data downstream along with
the respective notification messages (SNS).

Steps performed:

    - Retrieves data from S3 bucket
    - Converts the data from json to dataframe,
    - Ensures the mandatory columns are present and correctly typed
    - Appends the new output columns in zero state
    - Sends the dataframe to the method
    - Ensures the new columns are still present and correctly typed in the returned dataframe
    - Serialises the dataframe back to json
    - sends the data on via SQS
    - Notifies via SNS   
<hr>

## Methods

#### Calculate Enterprise Reference Count Method

**Name of Lambda:** aggregation_column_method

**Summary:** This method is responsible for grouping the data by a given column, region and period. It then aggregates on the specified column (e.g. enterprise_ref) creating a total (e.g. ent_ref_count) and then renames the column accordingly.

**Inputs:** The method requires the data which is output from imputation but filtered by the current period (done by wrangler) and contains all the following columns: (county/enterprise_ref/...), region, period.

**Outputs:** A JSON dict which contains a success marker and the aggregated data with the column count/sum.
<hr>

#### Calculate Top Two Method

**Name of Lambda:** aggregation_top2_wrangler

**Summary:** Takes a DataFrame in json format and uses the columns period, column* and total* to calculate the highest and second highest total within each period/column* combination. These are then appended as two new columns. Finally, the DataFrame is re-converted to json and sent on via SQS.

**Inputs:** This method requires a DataFrame in json format containing the following integer columns: "columns*", "period" and "total*"

**Outputs:** A JSON dict which contains a success marker and the input DataFrame with the following two columns appended: "largest_contributor" and "second_largest_contributor"
<hr>

#### Combiner
The combiner is used to join the outputs from the 3 aggregations back onto the original
 data. It is assumed that the imputed(or original if it didnt need imputing) data is 
 stored in an s3 bucket by the imputation module; and that each of the 3 aggregation 
 processes each write their output to sqs.<br><br>
 The combiner merely picks up the imputation data from s3, then 3 messages from the sqs
  queue. It joins these all together and sends onwards. The result of which is that the
   next module(disclosure) has the granular input data with the addition of aggregations 
   merged on.

*The exact column can be provided as a runtime variable.
