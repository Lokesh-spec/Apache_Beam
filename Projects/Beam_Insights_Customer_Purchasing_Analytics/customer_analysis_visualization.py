
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


age_vs_income_and_purchase_amount_df = pd.read_csv("Results\AgeVsIncome&PurchaseAmount-00000-of-00001.csv")
age_vs_loyality_df = pd.read_csv("Results\AgeVsLoyality-00000-of-00001.csv")
region_vs_purchase_amount_and_frequency_df = pd.read_csv("Results\RegionVsPurchaseAmount&Frequency-00000-of-00001.csv")

age_vs_income_and_purchase_amount_df = age_vs_income_and_purchase_amount_df.sort_values(by=["Age"])
age_vs_income_and_purchase_amount_header = list(age_vs_income_and_purchase_amount_df.columns) 
age_vs_income_and_purchase_amount_cell_values = [age_vs_income_and_purchase_amount_df[col].tolist() for col in age_vs_income_and_purchase_amount_df.columns]  

age_vs_income_and_purchase_amount_table = go.Figure(data=[go.Table(
    header=dict(values=age_vs_income_and_purchase_amount_header,  
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=age_vs_income_and_purchase_amount_cell_values, 
               fill_color='lavender',
               align='left'))
])

age_vs_income_and_purchase_amount_table.update_layout(
    title_text="Customer Age, Income, and Purchase Amount Overview", 
    title_x=0.5,  
    title_font=dict(size=24)  
)


age_vs_income_and_purchase_amount_fig = px.scatter(age_vs_income_and_purchase_amount_df, x='Age', y='Income', size='Purchase Amount', 
                 title='Bubble Chart of Income vs Age with Purchase Amount as Size',
                 labels={'Age': 'Age', 'Income': 'Income', 'Purchase Amount': 'Purchase Amount'})


age_vs_income_and_purchase_amount_fig.show()

age_vs_income_and_purchase_amount_table.show()


age_vs_loyality_df = age_vs_loyality_df.sort_values(by=["Age"])
age_vs_loyality_header = list(age_vs_loyality_df.columns) 
age_vs_loyality_cell_values = [age_vs_loyality_df[col].tolist() for col in age_vs_loyality_df.columns]  

age_vs_loyality_table = go.Figure(data=[go.Table(
    header=dict(values=age_vs_loyality_header,  
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=age_vs_loyality_cell_values, 
               fill_color='lavender',
               align='left'))
])

age_vs_loyality_table.update_layout(
    title_text="Customer Age Vs Loyality Overview", 
    title_x=0.5,  
    title_font=dict(size=24)  
)

age_vs_loyality_table.show()

age_vs_loyality_fig = px.line(age_vs_loyality_df, x="Age", y="Loyality", title='Age and Loyalty Score Relationship')
age_vs_loyality_fig.show()

age_vs_income_and_purchase_amount_fig.show()

age_vs_income_and_purchase_amount_table.show()

region_vs_purchase_amount_and_frequency_header = list(region_vs_purchase_amount_and_frequency_df.columns) 
region_vs_purchase_amount_and_frequency_cell_values = [region_vs_purchase_amount_and_frequency_df[col].tolist() for col in region_vs_purchase_amount_and_frequency_df.columns]  

region_vs_purchase_amount_and_frequency_table = go.Figure(data=[go.Table(
    header=dict(values=region_vs_purchase_amount_and_frequency_header,  
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=region_vs_purchase_amount_and_frequency_cell_values, 
               fill_color='lavender',
               align='left'))
])

region_vs_purchase_amount_and_frequency_table.update_layout(
    title_text="Region-wise Purchase Data", 
    title_x=0.5,  
    title_font=dict(size=24)  
)

region_vs_purchase_amount_and_frequency_table.show()


region_vs_purchase_amount_and_frequency_fig = go.Figure()

region_vs_purchase_amount_and_frequency_fig.add_trace(go.Bar(
    x=region_vs_purchase_amount_and_frequency_df['Region'],
    y=region_vs_purchase_amount_and_frequency_df['Purchase Amount'],
    name='Purchase Amount',
    marker_color='blue'
))

region_vs_purchase_amount_and_frequency_fig.add_trace(go.Bar(
    x=region_vs_purchase_amount_and_frequency_df['Region'],
    y=region_vs_purchase_amount_and_frequency_df['Purchase Frequency'],
    name='Purchase Frequency',
    marker_color='orange'
))



# Update layout
region_vs_purchase_amount_and_frequency_fig.update_layout(
    title='Purchase Amount and Frequency by Region',
    xaxis_title='Region',
    yaxis_title='Amount/Frequency',
    barmode='group'
)

region_vs_purchase_amount_and_frequency_fig.show()