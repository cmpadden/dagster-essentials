import os

import geopandas as gpd
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dagster import asset
from dagster_duckdb import DuckDBResource

from ..partitions import weekly_partition
from . import constants


@asset(
    deps=["taxi_trips", "taxi_zones"],
    group_name="metrics",
)
def manhattan_stats(database: DuckDBResource):
    query = """
    select
        zones.zone,
        zones.borough,
        zones.geometry,
        count(1) as num_trips,
    from trips
    left join zones on trips.pickup_zone_id = zones.zone_id
    where borough = 'Manhattan' and geometry is not null
    group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(
    deps=["manhattan_stats"],
    group_name="metrics",
)
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)





@asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition,
    group_name="metrics",
)
def trips_by_week(context, database: DuckDBResource):

    start_date = context.asset_partition_key_for_output()

    query = f"""
    select
      '{start_date}' as period,
      count(*) as num_trips,
      sum(passenger_count)::int as passenger_count,
      round(sum(total_amount), 2) as total_amount,
      round(sum(trip_distance), 2) as trip_distance
    from trips
    where
      pickup_datetime::date >= '{start_date}'
      and pickup_datetime::date <= '{start_date}'::date + interval '1 week'
    group by period
    order by period
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()

        # Updates the `start_date` period of data in the existing CSV, or writes to a
        # new file.
        #
        # NOTE: this may result in concurrency issues; it may be better to write the
        # partitioned data to separate files, or not use CSVs for this instance.
        path = constants.TRIPS_BY_WEEK_FILE_PATH
        if os.path.exists(path):
            # WARNING: this requires the dataset to fit into memory.
            df_existing = pd.read_csv(path)
            df_existing = df_existing[df_existing['period'] != start_date]
            df_existing = pd.concat([df_existing, df])
            df_existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
        else:
            df.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
