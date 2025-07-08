import uuid
import polars as pl

def clean_user_profile()->None:
    with open('user_profiles.csv','r',encoding='utf-8-sig') as f_in:
        with open('clean_user_profiles.csv','w') as f_out:
            for line in f_in.readlines():
                f_out.writelines(line.strip('\"\n\r')+'\n')
        

def load_profile()->pl.DataFrame:
    # Load cleaned profile data with schema
    return pl.read_csv(
        'clean_user_profiles.csv',
        schema={'user_id': pl.Int32, 'name': pl.Utf8, 'registration_date': pl.Date, 'location': pl.Utf8},
        try_parse_dates=True
    )


def load_event_data()->pl.DataFrame:    
    # Load event data with Required transformations on event DataFrame
    df_user_event = pl.read_json('user_events_20231026.json').with_columns(
        pl.col('timestamp').str.strptime(pl.Datetime,format="%Y-%m-%dT%H:%M:%SZ")
    ).with_columns([
        pl.col('timestamp').dt.date().alias('event_date'),
        pl.col('details').struct.field('page_url').alias('page_url'),
        pl.col('details').struct.field('button_id').alias('button_id'),
        pl.col('details').struct.field('item_id').alias('item_id'),
        pl.col('details').struct.field('price').alias('price'),
        pl.col('details').struct.field('quantity').alias('quantity'),
        pl.col('details').struct.json_encode().alias('details_raw')
    ])
    return df_user_event.with_columns(
        pl.arange(0, df_user_event.height).map_elements(lambda _: str(uuid.uuid4()), pl.String).alias("event_id") # adding event_id
    )

def join_profile_and_events(profile_dataframe:pl.DataFrame,events_dataframe:pl.DataFrame)->pl.DataFrame:
    # Join on user_id
    return events_dataframe.join(profile_dataframe, on='user_id',how='left')


def write_to_parquet(joined_data:pl.DataFrame)->None:
    # Write parquet partitioned by event_date
    joined_data.select([
        'event_id', 'user_id', 'name', 'location', 'registration_date',
        'event_type', 'timestamp', 'event_date', 'details_raw', 'page_url', 'button_id', 'item_id','price','quantity'
    ]).write_parquet('result_parquet', partition_by=['event_date'],compression='snappy')

def main():
    # Clean user profile file
    clean_user_profile()

    # Load profile and user events
    df_profile = load_profile()
    df_user_event = load_event_data()
    # join data 
    joined_data = join_profile_and_events(df_profile,df_user_event)
    # write to parquet
    write_to_parquet(joined_data)

if __name__=='__main__':
    main()
