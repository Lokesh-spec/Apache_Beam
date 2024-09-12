# Beam Insights Customer Purchasing Analytics

This project performs customer purchasing analytics using Apache Beam. It processes and visualizes customer data related to purchasing behaviors, with the resulting insights saved as CSV files and images.

## Project Structure

```
Beam_Insights_Customer_Purchasing_Analytics
         |
         |---Data
  	 |    |-----Customer_Purchasing_Behaviors.csv
         |---Results
         |    |-----Images
         |    |	      |------Age and Loyalty Score Relationship.png
         |    |	      |------Bubble Chart of Income vs Age with Purchase Amount as Size.png
         |    |       |------Customer Age Vs Loyalty Overview.png
         |    |       |------Customer Purchase Patterns by Age and Income.png
         |    |       |------Purchase Amount and Frequency by Region.png
         |    |       |------Region-wise Purchase Data.png
         |    |-----AgeVsIncome&PurchaseAmount-00000-of-00001.csv
         |    |-----AgeVsLoyality-00000-of-00001.csv
         |    |-----RegionVsPurchaseAmount&Frequency-00000-of-00001.csv
         |---Venv
         |---customer_analysis_data_pipeline.py	
         |---customer_analysis_visualization.py
         |---requirements.txt
```


## Files and Directories

- **Data/Customer_Purchasing_Behaviors.csv**: The input dataset containing customer purchasing behavior data.
  
- **Results/Images/**: Contains visualizations generated from the analysis.
  - `Age and Loyalty Score Relationship.png`: Shows the relationship between customer age and loyalty score.
  - `Bubble Chart of Income vs Age with Purchase Amount as Size.png`: A bubble chart visualizing income vs age, with purchase amount represented by the size of the bubbles.
  - `Customer Age Vs Loyalty Overview.png`: Overview of customer age distribution in relation to loyalty.
  - `Customer Purchase Patterns by Age and Income.png`: Insights into customer purchase patterns across different ages and income groups.
  - `Purchase Amount and Frequency by Region.png`: Visualizes purchase frequency and amount in different regions.
  - `Region-wise Purchase Data.png`: Visual breakdown of purchase data by region.
  
- **Results/CSV Files**: The processed CSV files containing aggregated and analyzed customer data.
  - `AgeVsIncome&PurchaseAmount-00000-of-00001.csv`: Age vs income and purchase amount data.
  - `AgeVsLoyality-00000-of-00001.csv`: Age vs loyalty score data.
  - `RegionVsPurchaseAmount&Frequency-00000-of-00001.csv`: Region-wise purchase amount and frequency.

- **Venv/**: Virtual environment used for the project.

- **customer_analysis_data_pipeline.py**: Python script that contains the data pipeline using Apache Beam to process customer data.

- **customer_analysis_visualization.py**: Python script that generates visualizations based on the processed data.

- **requirements.txt**: Specifies the Python dependencies required to run the project.

## Setup

1. Clone this repository to your local machine.
    ```cd Beam_Insights_Customer_Purchasing_Analytics```

2. Create and activate a virtual environment:
   ```
   python3 -m venv venv
   Windows use `venv\Scripts\activate`  # On Ubuntu use `source venv/bin/activate`
   ````
3. Install the required dependencies: 
    ```pip install -r requirements.txt```
4. Ensure that Apache Beam and other necessary packages are correctly installed.
    Data Processing
    ```python customer_analysis_data_pipeline.py --input Data\Customer_Purchasing_Behaviors.csv --output Results --runner DirectRunner```
    This will generate CSV outputs with aggregated data.
5. Visualization
    To generate visualizations from the processed data, run the visualization script:
    ```python customer_analysis_visualization.py```

