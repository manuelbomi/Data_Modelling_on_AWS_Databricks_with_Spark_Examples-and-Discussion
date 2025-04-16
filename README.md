# Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion

##### In this submission, we have shown a method by which data can be modelled on Spark using the appropriate data types in lieu of using the 'inferSchema' method. 

inferSchema parameter plays a critical role. It offers a seemingly simple choice: let Spark automatically guess the data types (inferSchema=True) or define them explicitly (inferSchema=False). But the decision between convenience and control can significantly impact your data processing pipeline. This blog dives deeper into inferSchema, its implications, and best practices for optimal data handling.

inferSchema=True (Default): This is the "easy button." Spark peeks at your data and attempts to infer the data types for each column based on the values it sees. It's a great option for exploratory data analysis (EDA) or quick prototyping when you're unfamiliar with the data structure.
