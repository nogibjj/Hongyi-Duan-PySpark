# Hongyi-Duan-PySpark

[![PySpark Data Processing](https://github.com/nogibjj/Hongyi-Duan-PySpark/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Hongyi-Duan-PySpark/actions/workflows/cicd.yml)

This repository, `Hongyi-Duan-PySpark`, is a project that leverages Apache Spark for analyzing and processing NBA team data. The project includes data loading, transformation, and statistical analysis of team data, with automated tests to verify the functionality of each component.

## Project Overview

This project is built to demonstrate how PySpark can be used to handle large datasets and perform statistical calculations efficiently. The core components include counting NBA team appearances and calculating statistical metrics for Elo probabilities. All processes are automated and validated through unit tests.

### Key Files

1. **main.py**:
   - This is the primary script that performs data processing using PySpark.
   - **Features**:
     - Initializes a Spark session.
     - Loads NBA team data from a CSV file in the `data/` directory.
     - Counts team appearances.
     - Calculates Elo probability statistics, including mean, standard deviation, minimum, and maximum values.

2. **test_main.py**:
   - Contains unit tests for verifying the functionality in `main.py`.
   - **Test Coverage**:
     - Spark session initialization.
     - CSV file reading.
     - Counting of team appearances.
     - Elo probability statistics calculation.

3. **data/nba_teams.csv**:
   - A sample dataset containing NBA team information for analysis.

4. **requirements.txt**:
   - Lists all dependencies required to run the project, including `pandas`, `pytest`, and `pyspark`.

5. **Makefile**:
   - Defines commands to automate testing using `make test`.
   - **Commands**:
     - `make test`: Runs all tests in `test_main.py` using `pytest` to validate the processing steps in `main.py`.

## Installation

To set up this project locally, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/nogibjj/Hongyi-Duan-PySpark.git
   cd Hongyi-Duan-PySpark
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure Apache Spark is set up and available in your PATH.

## Usage

1. **Run the Main Script**:
   - The main script performs data analysis on NBA team data.
   ```bash
   python main.py
   ```
   - This will initialize a Spark session, load the data from `data/nba_teams.csv`, and output team appearance counts along with Elo probability statistics.

2. **Run Tests**:
   - Run the tests using the Makefile command:
   ```bash
   make test
   ```
   - This will execute all unit tests in `test_main.py` and verify each step of the data processing pipeline.

## Results

The following results demonstrate the output of data processing and testing.

### 1. Team Appearance Counts
This table shows the number of appearances for each NBA team based on the dataset.

<img width="170" alt="4b9c69bfa62c150164490e15face3b5" src="https://github.com/user-attachments/assets/ae51ae26-c9cf-4004-9025-12f73131b476">

### 2. Elo Probability Statistics
This table presents the calculated mean, standard deviation, minimum, and maximum values for Elo probabilities in the dataset.

<img width="649" alt="0d47b6dfdb71a7a07bcd606ac798c18" src="https://github.com/user-attachments/assets/7f398726-fc35-40b1-9a7d-aa94eae03b16">

### 3. Test Results
This output verifies that all tests have passed successfully, confirming the correctness of Spark initiation, CSV reading, team appearance counting, and Elo probability statistics calculation.

<img width="672" alt="a004a46d8ea751f34162ece707118a9" src="https://github.com/user-attachments/assets/ccd52749-7fd8-49ca-a0c9-f78aab2ba44a">

## How It Works

### Data Processing Workflow

1. **Initialize Spark Session**:
   - The Spark session is created to allow distributed data processing.

2. **Load Data**:
   - Data from `nba_teams.csv` is loaded into a Spark DataFrame.

3. **Data Transformation**:
   - Team appearances are counted by grouping the data by team names.
   - Elo probabilities are calculated to analyze the performance of different teams, including metrics like mean, standard deviation, minimum, and maximum values.

4. **Output Results**:
   - The results are displayed in tabular format and saved as images in the `results/` folder for easy documentation and reference.

### Testing Workflow

- The `test_main.py` file runs tests to validate:
  - Proper Spark session initialization.
  - Successful data loading from the CSV file.
  - Correct counting of team appearances.
  - Accurate calculation of Elo probability statistics.

## Dependencies

The following libraries are required to run this project:

- `pandas`
- `pyspark`
- `pytest`
- `pytest-cov`

You can install all dependencies using:
```bash
pip install -r requirements.txt
```
