# Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion

##### Full code available here: https://github.com/manuelbomi/Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion/blob/main/Data_Modelling_on_AWS_Databricks_with_Spark_Examples_%26_Discussion.ipynb

##### In this submission, we have shown a manual method by which data can be modelled on Spark using the appropriate data types in lieu of using the 'inferSchema' method. We have also briefly shown (with Spark codes) how the createOrReplaceTempView() and the createGlobalTempView() methods can be used to create temporary views that can be shared among all sessions and keep alive. These temporary views are lazily evaluated and can be used to process petabytes of data just like a Hive table.

##### In addition, we have shown (with Spark codes) how ad-hoc data caching can be used during data analysis with Spark. 

##### Also, we have shown (with Spark codes) how User Defined Functions (UDFs) can be designed with Spark. 

##### The notebook concludes with showing how sorting, ordering, joining data (inner and outer joins) can be accomplished using Spark. 

##### Methods of collating, merging and imploding multiple data columns into a single column are also shown with Spark codes. 
  

## Challenges with using InferSchema method
InferSchema (inferSchema=True) works by letting Spark automatically infer or guess the type of the data to be anaylzed. It is a convenient method for initial exploratory data analyses (EDAs). It allows the data engineer to have a quick idea of the underlying data structure. 

While very convenient to use, especially for initial EDAs, automatic data inference using inferSchema can have some severe drawbacks:

    Schema Evolution Issues: When inferSchema is used to detect data types, the engineer will loose control over schema specification. It may be harder down the road to enforce data types and it will be harder still to eveolve the data types when new data are added to the initial data set. 
    Large Performance Overhead: When inferSchema is used to detect the data types, each records in the data may analyzed. This is very compute intensive and time consuming for large data sets.
    Wrong Inferencing: If some columns of data have mixed data types (for example a column that have integer and string data types mixed togethe), the inferSchema method can come up with an inaccurate data type in the schema  details. This can lead to huge errors and adverse processing issues down the road. 
    
A method of manually specifiying the schema and data type using Spark StructType() method is shown in the Fig below. The full code can be obtained in the code repo. 

![Image](https://github.com/user-attachments/assets/9c90097c-6176-4def-9c94-5cb3fc9c6cd0)

#### Benefits of Manually Specifying the Schema 
Modeling the data by defining the schema explicitly can have several advantages including:

    Processing Speed: Since their is no automatic inferencing by setting inferSchema to True, processing speed of the data can be faster. This can lead to huge savings in processing costs for large datasets.
    Improved Data Qulity and Accuracy:  Since the exact schema types are known, data will be of improved quality. No mismatcches, and no un-expected data conversion. Thus, it will be easier to document the data types for data governance activities. The data will also be richer, and it can be easily used in model training and code preparation.
 
#### Here, we show, with the aid of some Spark codes how User Defined Functions (UDFs) can be designed in Spark. (Detailed code is available here: https://github.com/manuelbomi/Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion/blob/main/Data_Modelling_on_AWS_Databricks_with_Spark_Examples_%26_Discussion.ipynb)  

 ---
Thank you for reading through

---

Author's Background

```
> [!NOTE]
Author's Name:  Emmanuel Oyekanlu
Skillset:   I have experience spanning several years in developing scalable enterprise data pipelines, architecting enterprise
data solutions, deep learning and LLM applications as well as deploying solutions (apps) on-prem and in the cloud.
I can be reached through: manuelbomi@yahoo.com
Website:  http://emmanueloyekanlu.com/
Publications:  https://scholar.google.com/citations?user=S-jTMfkAAAAJ&hl=en
LinkedIn:  https://www.linkedin.com/in/emmanuel-oyekanlu-6ba98616
Github:  https://github.com/manuelbomi

```

[![Icons](https://skillicons.dev/icons?i=aws,azure,gcp,scala,mongodb,redis,cassandra,kafka,anaconda,matlab,nodejs,django,py,c,anaconda,git,github,mysql,docker,kubernetes&theme=dark)](https://skillicons.dev)
![Image](https://github.com/user-attachments/assets/5d7c78f0-24cd-4487-920b-ee19689b3784)

#### Here, we show, with the aid of some Spark codes how several data columns can be imploded into a single column (Detailed code is available here: https://github.com/manuelbomi/Data_Modelling_on_AWS_Databricks_with_Spark_Examples-and-Discussion/blob/main/Data_Modelling_on_AWS_Databricks_with_Spark_Examples_%26_Discussion.ipynb) 

![Image](https://github.com/user-attachments/assets/3b4fa6bf-7d91-4b35-8165-abd9a1e0e094)
