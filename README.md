# ssdeep_comparisons_at_scale
ssdeep comparisons at scale in databricks
# Optimized SSDEEP in PySpark on Databricks

This Python project is an implementation of the article ["Optimizing SSDEEP for use at scale"](https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale) in PySpark using the Databricks platform. The primary objective of this project is to optimize and scale the SSDEEP fuzzy hashing algorithm for large-scale data processing and cybersecurity applications.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Performance](#performance)
- [Challenges](#challenges)

## Introduction

[SSDEEP](https://ssdeep-project.github.io/ssdeep/index.html) is a fuzzy hashing algorithm widely used for identifying similarities between digital artifacts like files and documents. It has been particularly useful in the cybersecurity domain for detecting malware and other malicious content. The article ["Optimizing SSDEEP for use at scale"](https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale) presents techniques to optimize the algorithm for processing large datasets, which is crucial for tackling cybersecurity threats at scale.

This project adapts the optimization techniques discussed in the article for a PySpark implementation, making it suitable for large-scale distributed data processing using the Databricks platform. With this implementation, users can leverage the power of Apache Spark and Databricks to perform SSDEEP-based similarity analysis on massive datasets in a highly efficient manner.

The key features of this project include:

1. **Scalability**: This implementation is designed to handle large datasets by leveraging the distributed computing capabilities of Apache Spark.
2. **Optimization**: The optimization techniques from the article are integrated into the PySpark implementation, resulting in faster and more efficient similarity computations.
3. **Ease of Use**: The project is designed for seamless integration with the Databricks platform, allowing users to easily deploy and use the solution in their workflows.
4. **Extensibility**: The implementation is designed to be easily extensible, allowing users to customize and enhance the solution as needed for their specific use cases.



## Installation

### 1. Clone the Repository
```
git clone https://github.com/rohan-ahire/ssdeep_comparisons_at_scale.git
cd ssdeep_comparisons_at_scale
```

### 2. Build the Wheel File
```
pip install wheel
python setup.py bdist_wheel
```
The wheel file will be created in the `dist` folder.

### 3. Upload the Wheel File to Your Databricks Workspace or Cloud Storage
Upload the wheel file to a location accessible by your Databricks cluster, such as DBFS, S3, or Azure Blob Storage.

### 4. Create the Cluster Init Script
Copy the cluster init script named [install_ssdeep.sh](./install_ssdeep.sh) to dbfs

### 5. Create databricks cluster
Create a new cluster or edit an existing one, and follow these steps:

- Attach the wheel file: In the "Libraries" section, click "Install New", choose "PyPI", and provide the path to the wheel file (e.g., dbfs:/your-python-package/dist/your_python_package-0.1.0-py3-none-any.whl)
- Add the init script: In the "Advanced Options" section, click on the "Init Scripts" tab, and add the path to the install_ssdeep.sh script (e.g., dbfs:/your-python-package/install_ssdeep.sh)
- Start the cluster, and it will run the install_ssdeep.sh script to install ssdeep along with its dependencies. Your custom package will also be installed on the cluster.

## Usage
This package contains four notebooks, which demonstrate different use cases for working with ssdeep hashes. These notebooks can be found in the notebooks folder of the repository.

1. **Transform ssdeep_hash to list of integers** - 
This notebook demonstrates how to transform an ssdeep hash into a list of integers for chunk and double chunk. Follow the instructions and code samples provided in the notebook to preprocess ssdeep hashes for further analysis.

Notebook: [transform_ssdeep_hash.py](./notebooks/transform_ssdeep_hash.py)


2. **Single Value Lookup** - 
This notebook demonstrates how to compare a given ssdeep hash with a master table of transformed ssdeep hashes using an optimized methodology for large-scale comparisons, as referenced in this Virus Bulletin article. The output will be a list of comparison results between the input hash and relevant hashes from the master table.

Notebook: [single_value_lookup.py](./notebooks/single_value_lookup.py)

3. **Multi Value Lookup** - 
This notebook extends the single value lookup functionality to support multiple input ssdeep hashes in the form of a table. It demonstrates how to efficiently compare multiple input ssdeep hashes with a master table of transformed ssdeep hashes.

Notebook: [multi_value_lookup.py](./notebooks/multi_value_lookup.py)

## Testing
This notebook demonstrates how to test the accuracy and completeness of the ssdeep comparisons. It compares the single value lookup methodology with a brute-force approach that performs a cross join between the input ssdeep hash and all ssdeep hashes in the database. The output consists of a comparison between the two methods, ensuring that all comparison results with a score greater than 0 are accurately captured. See the testing section of the below notebook.

Notebook: [single_value_lookup.py](./notebooks/single_value_lookup.py)


## Performance

The method from the article referenced here - https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale offers a more efficient approach to comparing ssdeep hashes in Spark. Unlike the traditional m*n operation involving cross joins, which can be time-consuming and computationally intensive, this technique considerably reduces the number of comparisons needed. As a result, we can effectively identify all ssdeep hashes with a similarity score greater than 0. The underlying datasets have inherent skew to be true representtive of real world datasets.

The table below demonstrates the performance improvements we've achieved in this iteration. As an initial attempt at applying this methodology in Spark, our work provides a valuable starting point for further exploration of ssdeep hash comparisons, which could ultimately facilitate clustering similar files.

| Experiment | Node Type    | dbu per/hr | \# Nodes | Table A (# Rows - Corpus) | Table B (# Rows - Look up values) | Execution Time (mins) |
| ---------- | ------------ | ---------- | -------- | ------------------------- | --------------------------------- | --------------------- |
| 1          | i3.xlarge    | 12.96      | 1        | 20000000                  | 1                                 | 0.2                   |
| 2          | i3.xlarge    | 20.96      | 5        | 20000000                  | 100                               | 1                     |
| 3          | i3.xlarge    | 30.96      | 10       | 20000000                  | 1000                              | 3.5                   |
| 4          | i3.xlarge    | 50.96      | 20       | 20000000                  | 10000                             | 15                    |
| 5          | i3.xlarge    | 110.96     | 50       | 20000000                  | 100000                            | 72                    |
| 6          | m5d.8x.large | 21.92      | 1        | 20000000                  | 1                                 | 0.5                   |
| 7          | m5d.8x.large | 21.92      | 1        | 20000000                  | 100                               | 0.5                   |
| 8          | m5d.8x.large | 21.92      | 1        | 20000000                  | 1000                              | 3                     |
| 9          | m5d.8x.large | 65.76      | 5        | 20000000                  | 10000                             | 7                     |
| 10         | m5d.8x.large | 175.36     | 15       | 20000000                  | 100000                            | 40                    |


## Challenges

While the method we've implemented offers significant improvements in ssdeep hash comparisons, there are a few challenges we've encountered along the way:

 - **Broadcast Join** : The technique relies on using broadcast join, where we broadcast the smaller lookup table. It is essential to set the broadcast join threshold appropriately to accommodate the size of the smaller table.
- **Skewed Join** : The join between the corpus and the lookup table can suffer if the underlying lookup table is highly skewed based on the chunk size join key. Skew can be addressed manually by salting the data or ensuring that the underlying tables are stored randomly, without any ordering based on the join key.
- **Array Intersection** : This technique also relies on the array_intersect Spark SQL function. We compared it with an alternative approach involving exploding arrays and performing a join. However, array_intersect proved to be faster and consumed fewer resources. Additionally, the explode and join technique encountered more data skew issues than array_intersect, due to the variable size of chunk and double chunk arrays, which amplifies skew.
- **Balancing Workload** : Our primary goal is to reduce processing time while using minimal resources (cost and time). A current challenge is to evenly distribute the workload across all Spark tasks, which would likely further decrease processing time.

In summary, this work serves as a impactful first attempt at implementing this methodology in Spark. We acknowledge the challenges and strive to continuously improve our solution, making ssdeep hash comparisons more efficient and effective.
