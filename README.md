
# es-aggregation-sg

## Wranglers

### Calculate Column (EntRef/County/..) Wrangler

The wrangler is responsible for preparing the data, invoking the lambda and then sending the data downstream along with the respective notification messages (SNS).

Steps performed:

    - Retrieves data From S3 bucket
    - Invokes method lambda
    - Puts the aggregated data in an S3 bucket
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
    - Saves the data in an S3 bucket 
    - Notifies via SNS   
<hr>

## Methods

#### Calculate Enterprise Reference Count Method

**Name of Lambda:** aggregation_column_method

**Summary:** This method is responsible for grouping the data by a given column, and region. It then aggregates on the specified column (e.g. enterprise_ref) creating a total (e.g. ent_ref_count) and then renames the column accordingly.

**Inputs:**
    event: {"RuntimeVariables":{ <br>
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference. <br>
        additional_aggregated_column - A column to aggregate by. e.g. Region. <br>
        aggregation_type - How we wish to do the aggregation. e.g. sum, count, nunique. <br>
        total_columns - The names of the columns to produce aggregations for. <br>
        cell_total_column - Name of column to rename total_column. <br>
 }}

**Outputs:** A JSON dict which contains a success marker and the aggregated data with the column count/sum. <br>
e.g. {"success": True/False, "checkpoint"/"error": 4/"Message"}
<hr>

#### Calculate Top Two Method

**Name of Lambda:** aggregation_top2_wrangler

**Summary:** Takes a DataFrame in json format and calculates the highest and second highest total within each unique combination of the aggregated_column and additional aggregated column (column names are adjustable in the runtime variables). These are then appended as two new columns. The DataFrame is saved to S3 as json and a notification sent on to the next module via SNS.

**Inputs:**
    event: {"RuntimeVariables":{ <br>
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference. <br>
        additional_aggregated_column - A column to aggregate by. e.g. Region. <br>
        total_columns - The names of the columns to produce aggregations for. <br>
    }}

**Outputs:** A JSON dict which contains a success marker and the input DataFrame with the following two columns appended: "largest_contributor" and "second_largest_contributor" <br>
e.g. {"success": True/False, "checkpoint"/"error": 4/"Message"}

<hr>

#### Combiner

The combiner is used to join the outputs from the 3 aggregations back onto the original data. It is assumed that the imputed(or original if it didnt need imputing) data is stored in an s3 bucket by the imputation module; and that each of the 3 aggregation processes each write their output to S3. <br>
The combiner merely picks up the imputation data and the 3 files from the other aggregation stages from s3. It joins these all together and sends onwards. The result of which is that the next module(disclosure) has the granular input data with the addition of aggregations merged on.

*The exact column can be provided as a runtime variable.
