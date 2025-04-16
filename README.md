# Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion

##### Full code available here: https://github.com/manuelbomi/Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion/blob/main/Data_Modelling_on_AWS_Databricks_with_Spark_Examples_%26_Discussion.ipynb

##### In this submission, we have shown a manual method by which data can be modelled on Spark using the appropriate data types in lieu of using the 'inferSchema' method. We have also briefly shown (with Spark codes) how the createOrReplaceTempView() and the createGlobalTempView() methods can be used to create temporary views that can be shared among all sessions and keep alive. These temporary views are lazily evaluated and can be used to process petabytes of data just like a Hive table, plus the benefit of using Spark SQL for querying data. 

##### In addition, we have shown (with Spark codes) how ad-hoc caching can be used during data analysis with Spark. 

##### Also, we have shown (with Spark codes) how User Defined Functions (UDFs) can be designed with Spark. 

##### The notebook concludes with showing how sorting, ordering, joining data (inner and outer joins) can be accomplished using Spark. 

##### Methods of collating, merging and imploding multiple data columns into a single column are also shown with Spark codes. 
  

#### Challenges with using InferSchema method
InferSchema (inferSchema=True) works by letting Spark automatically infer or guess the type of the data to be anaylzed. It is a convenient method for initial exploratory data analyses. It allows the data engineer to have a quick idea of the underlying data structure. 





The Catch with Automatic Inference

While convenient, automatic inference can have drawbacks:

    Performance Overhead: Analyzing data to infer types can be time-consuming, especially for large datasets.
    Inaccurate Inferences: Inconsistent data (e.g., a column containing both numbers and text) can lead to inaccurate data types, causing downstream processing issues.
    Limited Control: You relinquish control over data quality checks and schema evolution.

Benefits of Manual Schema Definition
inferSchema=False: This gives you more control. You define the schema beforehand using the StructType API, explicitly specifying the data types for each column. This is crucial for production-grade pipelines where performance and data integrity are paramount.

By defining the schema explicitly, you gain several advantages:

    Improved Performance: Avoids the overhead of inference, leading to faster processing.
    Enhanced Accuracy: Ensures data types are what you expect, preventing unexpected conversions.
    Data Quality and Validation: Allows you to incorporate data validation logic within the schema definition.
    Explicitness and Documentation: Clearly defines the expected data structure for better code clarity and collaboration.

Choosing the Right Approach: A Balancing Act

The optimal choice depends on your specific use case:

Use Automatic Inference for:

    Exploratory data analysis (EDA) when you’re unfamiliar with the data structure.
    Small datasets where the inference overhead is negligible.

Opt for Manual Schema Definition for:

    Production pipelines where performance and data quality are critical.
    Large datasets where accurate data type inferences are essential.
    Scenarios with potential inconsistencies or mixed data types within columns.

Beyond inferSchema: Optimizing Your Data Pipeline



![Image](https://github.com/user-attachments/assets/9c90097c-6176-4def-9c94-5cb3fc9c6cd0)



![Image](https://github.com/user-attachments/assets/5d7c78f0-24cd-4487-920b-ee19689b3784)



![Image](https://github.com/user-attachments/assets/3b4fa6bf-7d91-4b35-8165-abd9a1e0e094)
