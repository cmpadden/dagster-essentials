import duckdb
import geopandas as gpd
import plotly.express as px
import plotly.io as pio
from dagster import asset
from dagster_duckdb import DuckDBResource

from . import constants


@asset(
  deps=["taxi_trips", "taxi_zones"]
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
  deps=["taxi_trips"]
)
def trips_by_week(database: DuckDBResource):

    start_date = '2023-02-26'  # Sunday, February 26th
    end_date = '2023-04-01'  # Sunday, April 1st

    query = f"""
    select
      date_trunc('week', pickup_datetime) - interval '1 day' as period,
      count(*) as num_trips,
      sum(passenger_count)::int as passenger_count,
      round(sum(total_amount), 2) as total_amount,
      round(sum(trip_distance), 2) as trip_distance
    from trips
    where
      pickup_datetime::date >= '{start_date}'
      and pickup_datetime::date <= '{end_date}'
    group by period
    order by period
    """

    with database.get_connection() as conn:
        df = conn.execute(query).fetch_df()
        df.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
