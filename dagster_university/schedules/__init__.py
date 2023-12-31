from dagster import ScheduleDefinition
from ..jobs import trip_update_job, trips_by_week_update_job

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *",  # At 00:00 on day-of-month 5.
)

weekly_update_schedule = ScheduleDefinition(
    job=trips_by_week_update_job,
    cron_schedule="0 0 * * 1",  # At 00:00 on Monday.
)
