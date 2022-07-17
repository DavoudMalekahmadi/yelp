# Data Engineering Test

## Design Decisions
I created two python files:
### - import_raw_data.py
I decided to write cleaned raw data in parquet file format. Also, I decided to make a new column based on every field in the nested json.

### - calculate_aggregated_data.py
Containing some functions for aggregation and making the star schemas.
## How to Run the current project locally
- install requirements
```
pip install -r requirements.txt
```

- run task1.py 
```
spark-submit --master local[*] --deploy-mode client --py-files import_raw_data.py import_raw_data.py
```
- run task2.py
```
spark-submit --master local[*] --deploy-mode client --py-files calculate_aggregated_data.py calculate_aggregated_data.py
```