# ETL Insurance

A simple **ETL (Extract-Transform-Load)** workflow implemented in **Python** for insurance data processing.

This project demonstrates how to build a basic ETL pipeline that extracts data from source files, transforms it into a cleaned and usable format, and loads it into a target destination (e.g., database, CSV output, etc.). It’s designed as a starting point for working with insurance datasets and understanding ETL fundamentals.

---

##  Overview

ETL workflows are critical in data engineering for cleaning, reorganizing, and storing data for analysis or downstream systems. This project aims to:

- Introduce core ETL concepts using Python  
- Provide a reusable pipeline structure  
- Showcase simple transformations and data validation steps



---
## Project Workflow

This is the workflow of the project:

- Extract the data from the database with the use of SQLAlchemy
- Validate the extracted data with the use of great_expectations ,that is checking and testing for data quality checks
- Add the unique userid and datetime which is part the transformation
- Then loading it to Hopsworks for future ML work
  
---

## Software Engineering

 Some applications of software engineering in the project:
- Reusability - designing and creating own libraries that can be used to reduce code duplication in the workflow
- Modularity - Breaking down complex systems into smaller, manageable, and reusable libraries

---


