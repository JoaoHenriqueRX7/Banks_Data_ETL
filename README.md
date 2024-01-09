# Largest Banks Data ETL Process Using Python

## Introduction

This project automates an ETL process using Python to handle data on the world's largest banks. It involves extracting banking data from an archived web page, transforming it through currency conversion, and loading the processed data into a CSV file and an SQLite database.

## Project Overview

1. **Data Extraction:**
   - Scrape the list of largest banks from a historical snapshot of Wikipedia.

2. **Data Transformation:**
   - Use Pandas and NumPy to convert financial figures and perform currency exchange rate calculations.
   - Update DataFrame column names to enhance readability.

3. **Data Load:**
   - Export the transformed data into a CSV file.
   - Import the data into an SQLite database for persistent storage.

4. **Query and Log:**
   - Run SQL queries against the database.
   - Log the progress and any issues encountered throughout the ETL process.

## Technical Aspects

- Employs Python libraries like `requests`, `pandas`, `BeautifulSoup`, `numpy`, and `sqlite3`.
- Demonstrates expertise in data extraction, transformation, and database operations.
- Incorporates error handling and logging to ensure reliability.

## Execution Guide

- **Setup:**
  - Confirm installation of all necessary Python libraries.
  - Prepare the SQLite environment and define the database schema.

- **Running the Script:**
  - The ETL script is composed of discrete functions, each fulfilling a specific role in the ETL pipeline.
  - Run the script in sequence to perform the full ETL process or invoke individual functions for selective operation.

- **Logging:**
  - All operations are logged to a file, aiding in monitoring and debugging.

## Requirements

- Python (3.11.3 or later recommended)
- Python libraries: `pandas`, `requests`, `BeautifulSoup`, `numpy`, `sqlite3`

## Installation

Execute the following command to install the required libraries:

```sh
pip install pandas requests beautifulsoup4 numpy
```

## License

The source code is available under the MIT license. See LICENSE for more information.

## Acknowledgments

This project was inspired by various resources and similar projects in the field of data science. Special thanks to all contributors and the open-source community.

© Copyright 2023 João Henrique. All rights reserved.
