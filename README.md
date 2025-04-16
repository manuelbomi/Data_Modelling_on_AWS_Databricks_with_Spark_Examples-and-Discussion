# Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion

### Full code available here: https://github.com/manuelbomi/Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion/blob/main/Data_Modelling_on_AWS_Databricks_with_Spark_Examples_%26_Discussion.ipynb

##### In this submission, we have shown a method by which data can be modelled on Spark using the appropriate data types in lieu of using the 'inferSchema' method. 

inferSchema parameter plays a critical role. It offers a seemingly simple choice: let Spark automatically guess the data types (inferSchema=True) or define them explicitly (inferSchema=False). But the decision between convenience and control can significantly impact your data processing pipeline. This blog dives deeper into inferSchema, its implications, and best practices for optimal data handling.

inferSchema=True (Default): This is the "easy button." Spark peeks at your data and attempts to infer the data types for each column based on the values it sees. It's a great option for exploratory data analysis (EDA) or quick prototyping when you're unfamiliar with the data structure.

inferSchema=False: This gives you more control. You define the schema beforehand using the StructType API, explicitly specifying the data types for each column. This is crucial for production-grade pipelines where performance and data integrity are paramount.

The Catch with Automatic Inference

While convenient, automatic inference can have drawbacks:

    Performance Overhead: Analyzing data to infer types can be time-consuming, especially for large datasets.
    Inaccurate Inferences: Inconsistent data (e.g., a column containing both numbers and text) can lead to inaccurate data types, causing downstream processing issues.
    Limited Control: You relinquish control over data quality checks and schema evolution.

Benefits of Manual Schema Definition

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

Data ingestion in PySpark goes beyond inferSchema. Here are additional techniques for optimized workflows:

    Data Cleaning: Address data inconsistencies before ingestion by identifying and replacing invalid values, handling missing data, and enforcing data types.
    Partitioning: Partition data based on specific columns for faster query performance by allowing Spark to efficiently scan relevant data subsets.
    Optimized File Formats: Utilize file formats like Parquet for efficient storage and retrieval. Parquet offers schema enforcement, compression, and columnar storage, leading to faster reads and reduced processing overhead.
    Error Handling: Implement robust error handling to gracefully handle potential issues during data ingestion, such as invalid file formats, corrupted data, or unexpected data types.

![Image](https://github.com/user-attachments/assets/9c90097c-6176-4def-9c94-5cb3fc9c6cd0)



![Image](https://github.com/user-attachments/assets/5d7c78f0-24cd-4487-920b-ee19689b3784)



![Image](https://github.com/user-attachments/assets/3b4fa6bf-7d91-4b35-8165-abd9a1e0e094)
