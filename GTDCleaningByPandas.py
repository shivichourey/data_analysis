import pandas as pd
import random
import os
os.chdir(r"C:\Users\Arsalan Khan\OneDrive\Desktop\Project\Data")
import warnings
warnings.simplefilter('ignore')

#########################################################################################################################

df = pd.read_excel('globalterrorismdb_0522dist.xlsx')
df.rename(columns={'iyear': 'year', 'imonth': 'month', 'iday':'day'}, inplace=True)  #renaming the columns


##########################################################################################################################
#Converted the approxdate into a single date format and stored it in startdate and enddate columns

import re
from datetime import datetime

# Ensure all entries in the 'approxdate' column are strings
df['approxdate'] = df['approxdate'].astype(str)

#Function to parsedate
def parse_date(date_str):
    # Patterns to match different date formats
    patterns = [
        r'([A-Za-z]+) (\d+)-(\d+), (\d{4})',  # e.g., January 19-20, 1970
        r'([A-Za-z]+) (\d+) - ([A-Za-z]+) (\d+), (\d{4})',  # e.g., May 27 - June 5, 1970
        r'([A-Za-z]+) - ([A-Za-z]+), (\d{4})'  # e.g., April - May, 1980
    ]
    
    for pattern in patterns:
        match = re.match(pattern, date_str)
        if match:
            if len(match.groups()) == 4:
                # Single month date range
                month, start_day, end_day, year = match.groups()
                start_date = datetime.strptime(f"{month} {start_day} {year}", "%B %d %Y").strftime("%Y-%m-%d")
                try:
                    end_date = datetime.strptime(f"{month} {end_day} {year}", "%B %d %Y").strftime("%Y-%m-%d")
                except ValueError:
                    end_date = datetime.strptime(f"{month} {start_day} {year}", "%B %d %Y").replace(day=1, month=int(start_date[5:7])+1).strftime("%Y-%m-%d")
                return start_date, end_date
            elif len(match.groups()) == 5:
                # Multi-month date range
                start_month, start_day, end_month, end_day, year = match.groups()
                start_date = datetime.strptime(f"{start_month} {start_day} {year}", "%B %d %Y").strftime("%Y-%m-%d")
                try:
                    end_date = datetime.strptime(f"{end_month} {end_day} {year}", "%B %d %Y").strftime("%Y-%m-%d")
                except ValueError:
                    end_date = datetime.strptime(f"{end_month} {start_day} {year}", "%B %d %Y").replace(day=1, month=int(start_date[5:7])+1).strftime("%Y-%m-%d")
                return start_date, end_date
            elif len(match.groups()) == 3:
                # Month range without specific days
                start_month, end_month, year = match.groups()
                start_date = datetime.strptime(f"{start_month} 01 {year}", "%B %d %Y").strftime("%Y-%m-%d")
                end_date = datetime.strptime(f"{end_month} 01 {year}", "%B %d %Y")
                end_date = end_date.replace(day=1, month=end_date.month % 12 + 1) - pd.Timedelta(days=1)
                end_date = end_date.strftime("%Y-%m-%d")
                return start_date, end_date
    return None, None

# Apply the function to the dataframe and store results in new columns
df[['start_date', 'end_date']] = df['approxdate'].apply(lambda x: pd.Series(parse_date(x)) if pd.notnull(x) else (None, None))

################################################################################################################################

# Convert start_date and end_date columns to datetime
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Calculate the average date and store it in the approxdate column
def average_date(row):
    if pd.notnull(row['start_date']) and pd.notnull(row['end_date']):
        avg_date = row['start_date'] + (row['end_date'] - row['start_date']) / 2
        return avg_date.strftime("%Y-%m-%d")
    return None

df['approxdate'] = df.apply(average_date, axis=1)


################################################################################################################################

# Remove rows where the month value is 0
df.drop(df[df['month'] == 0].index, inplace=True)

# Extract the day from the approxdate column and store it in the day column
df['day'] = pd.to_datetime(df['approxdate']).dt.day

# Function to generate a random day for a given month
def random_day_for_month(month, year):
    if month in [1, 3, 5, 7, 8, 10, 12]:  # Months with 31 days
        return random.randint(1, 31)
    elif month in [4, 6, 9, 11]:  # Months with 30 days
        return random.randint(1, 30)
    elif month == 2:
        # Check for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return random.randint(1, 29)
        else:
            return random.randint(1, 28)

# Create the pred_date column with False as default
df['pred_date'] = False

# Iterate over the dataframe and replace day values
for index, row in df.iterrows():
    if pd.isna(row['day']) or row['day'] == 0:
        # Generate a random day
        random_day = random_day_for_month(row['month'], row['year'])
        df.at[index, 'day'] = random_day
        df.at[index, 'pred_date'] = True
        
# Combine day, month, and year into a date
df['date'] = pd.to_datetime(df[['year', 'month', 'day']])

# Drop rows where the longitude value is either 0 or NaN
df.drop(df[(df['longitude'] == 0) | (df['longitude'].isna())].index, inplace=True)


# Drop rows where the city value is either 0 or NaN
df.drop(df[(df['city'] == 0) | (df['city'].isna())].index, inplace=True)


# Drop rows where the multiple value is NaN
df.drop(df[(df['multiple'].isna())].index, inplace=True)

# Replace NaN (null) values in the specified columns with 'unknown'
df['natlty1_txt'].fillna('unknown', inplace=True)
df['natlty1'].fillna('unknown', inplace=True)

# Replace NaN (null) values in the specified columns with 'unknown'
df['natlty2_txt'].fillna('unknown', inplace=True)
df['natlty2'].fillna('unknown', inplace=True)

# Replace NaN (null) values in the specified columns with 'unknown'
df['natlty3_txt'].fillna('unknown', inplace=True)
df['natlty3'].fillna('unknown', inplace=True)

# Replace NaN (null) values in the specified columns with 'unknown'
df['gsubname'].fillna('unknown', inplace=True)
df['gsubname2'].fillna('unknown', inplace=True)
df['gsubname3'].fillna('unknown', inplace=True)
df['gname2'].fillna('unknown', inplace=True)
df['gname3'].fillna('unknown', inplace=True)
df['motive'].fillna('unknown', inplace=True)

df['guncertain2'].fillna(-1, inplace=True)
df['guncertain3'].fillna(-1, inplace=True)
df['claimed'].fillna(-1, inplace=True)
df['compclaim'].fillna(-1, inplace=True)
df['weapsubtype1'].fillna(-1, inplace=True)
df['weaptype2'].fillna(-1, inplace=True)
df['weapsubtype2'].fillna(-1, inplace=True)
df['weaptype3'].fillna(-1, inplace=True)
df['weapsubtype3'].fillna(-1, inplace=True)
df['weaptype4'].fillna(-1, inplace=True)
df['weapsubtype4'].fillna(-1, inplace=True)
df['weapdetail'].fillna('unknown', inplace=True)
df['nkill'].fillna(-1, inplace=True)
df['nkillus'].fillna(-1, inplace=True)
df['nkillter'].fillna(-1, inplace=True)
df['nwound'].fillna(-1, inplace=True)
df['nwoundus'].fillna(-1, inplace=True)
df['nwoundte'].fillna(-1, inplace=True)
df['summary'].fillna('Unknown', inplace=True)
df['targsubtype1'].fillna(-1,inplace=True)
df['targsubtype1_txt'].fillna('Unknown',inplace=True)
df['corp1'].fillna('Unknown', inplace=True)
df['target1'].fillna('Unknown', inplace=True)
df['guncertain1'].fillna(-1,inplace=True)
df['claimmode'].fillna(-1,inplace=True)
df['claimmode_txt'].fillna('Unknown', inplace=True)
df['weapsubtype1_txt'].fillna('Unknown', inplace=True)
df['propvalue'].fillna(-1,inplace=True)


# fill ishostkid with -9 
df["ishostkid"].fillna(-9, inplace=True)

# fill nhostkid with -99
df["nhostkid"].fillna(-99, inplace=True)

# look at nhours and ndays
df["nhours"].loc[df.nhours == 999] = -99
df["nhours"].fillna(-99, inplace = True)
df["ndays"].fillna(-99, inplace = True)



# fill ransom with 0
df["ransom"].fillna(0, inplace = True)
df["ransomamt"].fillna(-99, inplace = True)
df["ransomamtus"].fillna(-99, inplace = True)
df["ransompaid"].fillna(-99, inplace = True)
df["ransompaidus"].fillna(-99, inplace = True)





# fill nperps value with nperpcap value if we have a nperpcap value
df['nperps'].fillna(df['nperpcap'], inplace=True)

# fill na values in nperps and nperpcap with -99
df['nperps'].fillna(-99, inplace=True)
df['nperpcap'].fillna(-99, inplace=True)







df.drop(columns=['ransomnote', 'scite1', 'scite2', 'scite3', 'start_date', 'attacktype2', 'attacktype2_txt', 'attacktype3', 'attacktype3_txt', 'targtype2', 'targtype2_txt', 'targsubtype2', 'targsubtype2_txt', 'corp2', 'natlty2', 'natlty2_txt', 'targtype3', 'targtype3_txt', 'targsubtype3', 'targsubtype3_txt', 'corp3', 'natlty3', 'natlty3_txt', 'gname2', 'gsubname2', 'gname3', 'gsubname3', 'guncertain2', 'guncertain3', 'claim2', 'claimmode2', 'claimmode2_txt', 'claim2', 'claimmode2','end_date', 'claimmode2_txt', 'weaptype2', 'weaptype2_txt', 'weapsubtype2', 'weapsubtype2_txt', 'weaptype3', 'weaptype3_txt', 'weapsubtype3', 'weapsubtype3_txt', 'weaptype4', 'weaptype4_txt', 'weapsubtype4', 'weapsubtype4_txt', 'nwoundus', 'nkillus', 'nhostkidus', 'divert', 'kidhijcountry', 'ransomnote', 'addnotes', 'scite1', 'scite2', 'scite3', 'dbsource'], inplace=True)
df.drop(columns=['approxdate','location','alternative','alternative_txt','target2','target3','claim3','claimmode3_txt','claimmode3','propextent','propextent_txt','propcomment'],inplace=True)




















# Save the cleaned data to an Excel file
df.to_excel('cleaned_data.xlsx', index=False)


df_nan = df.loc[:,df.isna().any()].isna().sum()

# Count the columns with NaN or zero values
df_zero_nan = df.loc[:, df.isna().any() | (df == 0).any()].apply(lambda x: x.isna().sum() + (x == 0).sum())


