# ssdeep_comparisons_at_scale
ssdeep comparisons at scale in databricks
# Optimized SSDEEP in PySpark on Databricks

This Python project is an implementation of the article ["Optimizing SSDEEP for use at scale"](https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale) in PySpark using the Databricks platform. The primary objective of this project is to optimize and scale the SSDEEP fuzzy hashing algorithm for large-scale data processing and cybersecurity applications.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)

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
