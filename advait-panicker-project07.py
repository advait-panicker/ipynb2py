


import pandas as pd
def get_noaa_data (myyear: int) -> pd.DataFrame:
    """
    This function accepts a 4-digit year as input, and returns a data frame that contains the NOAA data for that year

    Args:
    myyear (int): This is a 4-digit year for which we will load a data frame to be returned.

    Returns:
    myDF (pd.DataFrame): This is the data frame that contains the NOAA data for that year
    """
    myfilepath = f'/anvil/projects/tdm/data/noaa/{myyear}.csv'
    mycolumnnames=["id","date","element_code","value","mflag","qflag","sflag","obstime"]
    myDF = pd.read_csv(myfilepath, names=mycolumnnames)
    myDF['date'] = pd.to_datetime(myDF['date'], format="%Y%m%d")
    return myDF
df = get_noaa_data(1880)
us_records_1880 = df[df['id'].str.startswith('US')]
len(us_records_1880)
df = get_noaa_data(1881)
us_records_1881 = df[df['id'].str.startswith('US')]
len(us_records_1881)
df = get_noaa_data(1882)
us_records_1882 = df[df['id'].str.startswith('US')]
len(us_records_1882)
df = get_noaa_data(1883)
us_records_1883 = df[df['id'].str.startswith('US')]
len(us_records_1883)
mydict = {1880: 48428, 1881: 48196, 1882: 50664, 1883: 52363}

def get_noaa_data_us_years(lst : list) -> dict:
    """
    Accepts a list of 4-digit integers (years), and returns a dictionary containing the number of US entries per year in the list

    Args:
    lst (list): a list of 4-digit integers (years)

    Returns:
    dict: the number of US entries per year in the list
    """
    output = {}
    for year in lst:
        df = get_noaa_data(year)
        us_records = df[df['id'].str.startswith('US')]
        output[year] = len(us_records)
    return output
get_noaa_data_us_years(list(range(1880, 1884)))

dict([key, mydict[key]] for key in sorted(get_noaa_data_us_years(list(range(1880, 1884))), key=mydict.get, reverse=True))
def get_noaa_data_us_years_reverse(lst: list) -> dict:
    """
    Accepts a list of 4-digit integers (years), and returns a dictionary containing the number of US entries per year in the list, in descending order of value

    Args:
    lst (list): a list of 4-digit integers (years)

    Returns:
    dict: the number of US entries per year in the list, in descending order of value
    """
    return dict([key, mydict[key]] for key in sorted(get_noaa_data_us_years(list(range(1880, 1884))), key=mydict.get, reverse=True))
get_noaa_data_us_years_reverse(list(range(1880, 1884)))


df = get_noaa_data(1880)
df1 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
len(df1)
df = get_noaa_data(1881)
df2 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
len(df2)
df = get_noaa_data(1882)
df3 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
len(df3)
df = get_noaa_data(1883)
df4 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
len(df4)
mydict = {1880: len(df1), 1881: len(df2), 1882: len(df3), 1883: len(df4)}
def get_noaa_data_us_years_snow(lst: list) -> dict:
    """
    Accepts a list of 4-digit integers (years), and returns a dictionary containing the number of US entries with snow per year in the list

    Args:
    lst (list): a list of 4-digit integers (years)

    Returns:
    dict: the number of US entries with snow per year in the list, in descending order of value
    """
    output = {}
    for year in lst:
        df = get_noaa_data(year)
        df = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
        output[year] = len(df)
    return output
get_noaa_data_us_years_snow(list(range(1880, 1884)))


df = get_noaa_data(1880)
df1 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df1.groupby('id').size().idxmax()
df = get_noaa_data(1881)
df2 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df2.groupby('id').size().idxmax()
df = get_noaa_data(1882)
df3 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df3.groupby('id').size().idxmax()
df = get_noaa_data(1883)
df4 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df4.groupby('id').size().idxmax()
mydict = {
    1880: df1.groupby('id').size().idxmax(),
    1881: df2.groupby('id').size().idxmax(),
    1882: df3.groupby('id').size().idxmax(),
    1883: df4.groupby('id').size().idxmax(),
}
mydict
def get_noaa_data_us_years_snow_id_max_days(lst: list) -> dict:
    """
    Accepts a list of 4-digit integers (years), and returns a dictionary containing the ID with the greatest number of snow days per year with snow in the US

    Args:
    lst (list): a list of 4-digit integers (years)

    Returns:
    dict: ID with the the greatest number of snow days per year with snow in the US
    """
    output = {}
    for year in lst:
        df = get_noaa_data(1883)
        df = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
        output[year] = df.groupby('id').size().idxmax()
    return output
get_noaa_data_us_years_snow_id_max_days(list(range(1880, 1884)))


df = get_noaa_data(1880)
df1 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df1.groupby('id')['value'].sum().idxmax()
df1.groupby('id')['value'].sum()
df = get_noaa_data(1881)
df2 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df2.groupby('id')['value'].sum().idxmax()
df = get_noaa_data(1882)
df3 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df3.groupby('id')['value'].sum().idxmax()
df = get_noaa_data(1884)
df3 = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
df3.groupby('id')['value'].sum().idxmax()
mydict = {
    1880: df1.groupby('id')['value'].sum().idxmax(),
    1881: df2.groupby('id')['value'].sum().idxmax(),
    1882: df3.groupby('id')['value'].sum().idxmax(),
    1883: df4.groupby('id')['value'].sum().idxmax()
}
mydict
def get_noaa_data_us_years_snow_id_max_amount(lst: list) -> dict:
    """
    Accepts a list of 4-digit integers (years), and returns a dictionary containing the ID with the greatest amount of snow per year with snow in the US

    Args:
    lst (list): a list of 4-digit integers (years)

    Returns:
    dict: ID with the the greatest amount of snow per year with snow in the US
    """
    output = {}
    for year in lst:
        df = get_noaa_data(year)
        df = df[(df['id'].str.startswith('US')) & (df['element_code'] == 'SNOW') & (df['value'] > 0)]
        output[year] = df.groupby('id')['value'].sum().idxmax()
    return output
get_noaa_data_us_years_snow_id_max_amount(list(range(1880, 1884)))


