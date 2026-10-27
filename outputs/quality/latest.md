# Data quality — latest validation

Written by `python -m src.quality` (Great Expectations 1.24.1) from the files of the last pipeline run. Deterministic: no timestamps or run ids; those go to `outputs/quality/history/` (git-ignored). Rules come from [`metadata/catalog.yaml`](../../metadata/catalog.yaml); `DQ-n` marks an expectation that duplicates invariant n of `tests/test_data_quality.py`. A failing **gate** stops the pipeline; a **warning** is reported only.

**Result: PASS** — 278 expectations: 278 passed, 0 failed.

| Suite | Expectations | Passed | Gate failures | Warnings |
|---|---:|---:|---:|---:|
| raw | 39 | 39 | 0 | 0 |
| processed | 164 | 164 | 0 | 0 |
| outputs | 75 | 75 | 0 | 0 |

## raw

| Check | What | Expectation | Kind | Result | Observed |
|---|---|---|---|---|---|
| `contacts.columns` | columns match the catalog, in order (DQ-1) | `expect_table_columns_to_match_ordered_list` | gate | pass | 13 values |
| `contacts.rows` | rows in [3,500,000, 5,500,000] (DQ-8) | `expect_table_row_count_to_be_between` | gate | pass | 4,740,671 |
| `contacts.primary_key` | primary key (contact_id) is unique (DQ-10) | `expect_column_values_to_be_unique` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.contact_id.not_null` | contact_id has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.contact_id.pattern` | contact_id matches ^CT-[0-9]{7,}$ | `expect_column_values_to_match_regex` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.timestamp.not_null` | timestamp has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.date.not_null` | date has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.interval_start.not_null` | interval_start has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.hour.not_null` | hour has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.hour.range` | hour in [0, 23] | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.day_of_week.not_null` | day_of_week has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.day_of_week.allowed` | day_of_week only takes allowed values (DQ-5) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.contact_reason.not_null` | contact_reason has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.contact_reason.allowed` | contact_reason only takes allowed values (DQ-5) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.customer_segment.not_null` | customer_segment has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.customer_segment.allowed` | customer_segment only takes allowed values (DQ-5) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.handle_time_seconds.not_null` | handle_time_seconds has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.handle_time_seconds.range` | handle_time_seconds in [30, 3,600] (DQ-3) | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.irop_flag.not_null` | irop_flag has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.resolved_first_contact.not_null` | resolved_first_contact has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.repeat_contact.not_null` | repeat_contact has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.abandoned.not_null` | abandoned has no nulls (DQ-2) | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.handle_time_seconds.integer` | handle_time_seconds is an integer type (DQ-3) | `expect_column_values_to_be_of_type` | gate | pass | int64 |
| `contacts.contact_reason.complete` | every allowed contact_reason value occurs (DQ-4) | `expect_column_distinct_values_to_equal_set` | gate | pass | 0 of 0 rows unexpected |
| `contacts.customer_segment.complete` | every allowed customer_segment value occurs (DQ-5) | `expect_column_distinct_values_to_equal_set` | gate | pass | 0 of 0 rows unexpected |
| `contacts.day_of_week.complete` | every allowed day_of_week value occurs (DQ-5) | `expect_column_distinct_values_to_equal_set` | gate | pass | 0 of 0 rows unexpected |
| `contacts.flags.abandoned_not_resolved` | no abandoned contact is a first-contact resolution (DQ-6) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.flags.repeat_not_resolved` | no repeat contact is a first-contact resolution (DQ-6) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.bucket.start_le_timestamp` | interval_start <= timestamp (DQ-9) | `expect_column_pair_values_a_to_be_greater_than_b` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.bucket.within_interval` | timestamp falls inside its 30-minute interval (DQ-9) | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.bucket.is_floor` | interval_start is the timestamp floored to the interval (DQ-9) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.hour.matches_timestamp` | hour matches the timestamp (DQ-11) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.day_of_week.matches_timestamp` | day_of_week matches the timestamp (DQ-11) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.date.matches_timestamp` | date matches the timestamp (DQ-11) | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts.reason_share.min` | no reason below 1% of contacts (DQ-4) | `expect_column_min_to_be_between` | gate | pass | 9.325304371 |
| `contacts.reason_share.max` | no reason above 60% of contacts (DQ-4) | `expect_column_max_to_be_between` | gate | pass | 26.16886934 |
| `contacts.reason_share.total` | reason shares sum to 100% (±0.5) (DQ-4) | `expect_column_sum_to_be_between` | gate | pass | 100 |
| `contacts.irop.events` | at least 10 distinct IROP events (runs of consecutive IROP days) (DQ-7) | `expect_table_row_count_to_be_between` | gate | pass | 12 |
| `contacts.irop.spread` | IROP events span more than 30 days (DQ-7) | `expect_column_max_to_be_between` | gate | pass | 293 |

## processed

| Check | What | Expectation | Kind | Result | Observed |
|---|---|---|---|---|---|
| `forecast.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | date, forecast, forecast_lower, forecast_upper, actual, segment |
| `forecast.rows` | rows = 60 | `expect_table_row_count_to_equal` | gate | pass | 60 |
| `forecast.primary_key` | primary key (date) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 60 rows unexpected |
| `forecast.date.not_null` | date has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast.not_null` | forecast has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast.range` | forecast >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast_lower.not_null` | forecast_lower has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast_lower.range` | forecast_lower >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast_upper.not_null` | forecast_upper has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast_upper.range` | forecast_upper >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `forecast.actual.range` | actual >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `forecast.segment.not_null` | segment has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 60 rows unexpected |
| `forecast.segment.allowed` | segment only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 60 rows unexpected |
| `forecast.upper_ge_forecast` | forecast_upper >= forecast | `expect_column_pair_values_a_to_be_greater_than_b` | gate | pass | 0 of 60 rows unexpected |
| `forecast.forecast_ge_lower` | forecast >= forecast_lower | `expect_column_pair_values_a_to_be_greater_than_b` | gate | pass | 0 of 60 rows unexpected |
| `intraday_profile.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | interval_index, interval_start, fraction |
| `intraday_profile.rows` | rows = 48 | `expect_table_row_count_to_equal` | gate | pass | 48 |
| `intraday_profile.primary_key` | primary key (interval_index) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.interval_index.not_null` | interval_index has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.interval_index.range` | interval_index in [0, 47] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.interval_start.not_null` | interval_start has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.fraction.not_null` | fraction has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.fraction.range` | fraction in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `intraday_profile.fraction.sum_is_one` | fractions sum to 1 within 1e-9 | `expect_column_sum_to_be_between` | gate | pass | 1 |
| `forecast_metrics.fields` | has exactly the catalog's fields | `expect_table_columns_to_match_set` | gate | pass | 9 values |
| `forecast_metrics.backend.not_null` | backend has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.backend.allowed` | backend only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.mape_all.not_null` | mape_all has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.mape_all.range` | mape_all >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.rmse_all.not_null` | rmse_all has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.rmse_all.range` | rmse_all >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.mape_normal.not_null` | mape_normal has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.mape_normal.range` | mape_normal >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.rmse_normal.not_null` | rmse_normal has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.rmse_normal.range` | rmse_normal >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_test.not_null` | n_test has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_test.range` | n_test >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_normal.not_null` | n_normal has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_normal.range` | n_normal >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_holiday.not_null` | n_holiday has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_holiday.range` | n_holiday >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_irop.not_null` | n_irop has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_irop.range` | n_irop >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `forecast_metrics.n_test` | scored on the whole held-out window (expected 30) | `expect_column_max_to_be_between` | gate | pass | 30 |
| `forecast_metrics.mape_normal_target` | normal-day MAPE under the spec's 10% target | `expect_column_max_to_be_between` | warning | pass | 9.674897384 |
| `staffing_plan.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | 11 values |
| `staffing_plan.rows` | rows = 1,440 | `expect_table_row_count_to_equal` | gate | pass | 1,440 |
| `staffing_plan.primary_key` | primary key (date, interval_index) is unique | `expect_compound_columns_to_be_unique` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.date.not_null` | date has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.interval_index.not_null` | interval_index has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.interval_index.range` | interval_index in [0, 47] | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.interval_start.not_null` | interval_start has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.volume.not_null` | volume has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.volume.range` | volume >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.aht_seconds.not_null` | aht_seconds has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.aht_seconds.range` | aht_seconds in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.offered_load_erlangs.not_null` | offered_load_erlangs has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.offered_load_erlangs.range` | offered_load_erlangs >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.required_agents.not_null` | required_agents has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.required_agents.range` | required_agents >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.scheduled_agents.not_null` | scheduled_agents has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.scheduled_agents.range` | scheduled_agents >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_sl.not_null` | predicted_sl has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_sl.range` | predicted_sl in [0.8, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_asa_seconds.not_null` | predicted_asa_seconds has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_asa_seconds.range` | predicted_asa_seconds >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_occupancy.not_null` | predicted_occupancy has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.predicted_occupancy.range` | predicted_occupancy in [0, 0.9] | `expect_column_values_to_be_between` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.scheduled_ge_required` | scheduled agents >= required agents in every slot | `expect_column_pair_values_a_to_be_greater_than_b` | gate | pass | 0 of 1,440 rows unexpected |
| `staffing_plan.dates_match_forecast` | plans exactly the forecast's future dates | `expect_column_distinct_values_to_equal_set` | gate | pass | 0 of 0 rows unexpected |
| `optimize_summary.fields` | has exactly the catalog's fields | `expect_table_columns_to_match_set` | gate | pass | 30 values |
| `optimize_summary.n_days.not_null` | n_days has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.n_days.range` | n_days >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.opt_hours.not_null` | cost.opt_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.opt_hours.range` | cost.opt_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.opt_cost.not_null` | cost.opt_cost has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.opt_cost.range` | cost.opt_cost >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.peak_scheduled.not_null` | cost.peak_scheduled has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.peak_scheduled.range` | cost.peak_scheduled >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.flat_hours.not_null` | cost.flat_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.flat_hours.range` | cost.flat_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.flat_cost.not_null` | cost.flat_cost has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.flat_cost.range` | cost.flat_cost >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.saved_hours.not_null` | cost.saved_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.saved_dollars.not_null` | cost.saved_dollars has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.pct_reduction.not_null` | cost.pct_reduction has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.pct_reduction.range` | cost.pct_reduction in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.block_hours.not_null` | cost.block_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.block_hours.range` | cost.block_hours >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_hours.not_null` | cost.realistic_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_hours.range` | cost.realistic_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_cost.not_null` | cost.realistic_cost has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_cost.range` | cost.realistic_cost >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_saved_hours.not_null` | cost.realistic_saved_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_saved_dollars.not_null` | cost.realistic_saved_dollars has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_pct_reduction.not_null` | cost.realistic_pct_reduction has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.cost.realistic_pct_reduction.range` | cost.realistic_pct_reduction in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.deflectable_share.not_null` | deflectable_share has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.deflectable_share.range` | deflectable_share in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.deflection_factor.not_null` | deflection_factor has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.deflection_factor.range` | deflection_factor in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.def_hours.not_null` | def_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.def_hours.range` | def_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.def_saved_hours.not_null` | def_saved_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.def_saved_dollars.not_null` | def_saved_dollars has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.aht_irop.not_null` | aht_irop has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.aht_irop.range` | aht_irop in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.irop_peak.not_null` | irop_peak has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.irop_peak.range` | irop_peak >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.normal_peak.not_null` | normal_peak has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.normal_peak.range` | normal_peak >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.irop_day_hours.not_null` | irop_day_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.irop_day_hours.range` | irop_day_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.normal_day_hours.not_null` | normal_day_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.normal_day_hours.range` | normal_day_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.opt_min_sl.not_null` | opt_min_sl has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.opt_min_sl.range` | opt_min_sl in [0.8, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.opt_mean_occ.not_null` | opt_mean_occ has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.opt_mean_occ.range` | opt_mean_occ in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_min.not_null` | flat_occ_min has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_min.range` | flat_occ_min in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_mean.not_null` | flat_occ_mean has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_mean.range` | flat_occ_mean in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_max.not_null` | flat_occ_max has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.flat_occ_max.range` | flat_occ_max in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `optimize_summary.n_days` | covers the whole horizon (expected 30) | `expect_column_max_to_be_between` | gate | pass | 30 |
| `validation.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | 14 values |
| `validation.rows` | rows = 4 | `expect_table_row_count_to_equal` | gate | pass | 4 |
| `validation.primary_key` | primary key (name) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 4 rows unexpected |
| `validation.name.not_null` | name has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.name.allowed` | name only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4 rows unexpected |
| `validation.load.not_null` | load has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.load.range` | load >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.agents.not_null` | agents has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.agents.range` | agents >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_sl.not_null` | pred_sl has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_sl.range` | pred_sl in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl.not_null` | sim_sl has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl.range` | sim_sl in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl_std.not_null` | sim_sl_std has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl_std.range` | sim_sl_std >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl_se.not_null` | sim_sl_se has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_sl_se.range` | sim_sl_se >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_asa.not_null` | pred_asa has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_asa.range` | pred_asa >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_asa.not_null` | sim_asa has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_asa.range` | sim_asa >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_occ.not_null` | pred_occ has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.pred_occ.range` | pred_occ in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_occ.not_null` | sim_occ has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sim_occ.range` | sim_occ in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.n.not_null` | n has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.n.range` | n >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.reps.not_null` | reps has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.reps.range` | reps >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `validation.sl_diff.not_null` | sl_diff has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `validation.sl_diff.range` | sl_diff in [0, 0.03] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `forecast.date.first_day` | starts on the first held-out day (days since SIM_START) (expected 335) | `expect_column_max_to_be_between` | gate | pass | 335 |
| `forecast.date.last_day` | ends on the last day of the horizon (days since SIM_START) (expected 394) | `expect_column_max_to_be_between` | gate | pass | 394 |
| `forecast.date.no_gaps` | consecutive dates, no gaps or repeats | `expect_column_values_to_be_in_set` | gate | pass | 0 of 59 rows unexpected |
| `forecast.test.days` | 30 held-out test days | `expect_table_row_count_to_equal` | gate | pass | 30 |
| `forecast.test.actual_present` | every test day has an actual | `expect_column_values_to_not_be_null` | gate | pass | 0 of 30 rows unexpected |
| `forecast.future.days` | 30 future days | `expect_table_row_count_to_equal` | gate | pass | 30 |
| `forecast.future.actual_empty` | no future day has an actual | `expect_column_values_to_be_null` | gate | pass | 0 of 30 rows unexpected |

## outputs

| Check | What | Expectation | Kind | Result | Observed |
|---|---|---|---|---|---|
| `daily_volume.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | date, actual, forecast, lower, upper |
| `daily_volume.rows` | rows = 395 | `expect_table_row_count_to_equal` | gate | pass | 395 |
| `daily_volume.primary_key` | primary key (date) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 395 rows unexpected |
| `daily_volume.date.not_null` | date has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 395 rows unexpected |
| `daily_volume.actual.range` | actual >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 395 rows unexpected |
| `daily_volume.forecast.range` | forecast >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 395 rows unexpected |
| `daily_volume.lower.range` | lower >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 395 rows unexpected |
| `daily_volume.upper.range` | upper >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 395 rows unexpected |
| `interval_staffing.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | interval, volume, required, scheduled, SL, occupancy |
| `interval_staffing.rows` | rows = 48 | `expect_table_row_count_to_equal` | gate | pass | 48 |
| `interval_staffing.primary_key` | primary key (interval) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.interval.not_null` | interval has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.volume.not_null` | volume has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.volume.range` | volume >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.required.not_null` | required has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.required.range` | required >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.scheduled.not_null` | scheduled has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.scheduled.range` | scheduled >= 1 | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.SL.not_null` | SL has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.SL.range` | SL in [0.8, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.occupancy.not_null` | occupancy has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `interval_staffing.occupancy.range` | occupancy in [0, 0.9] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `reason_mix.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | reason, contacts, avg_AHT, repeat_rate |
| `reason_mix.rows` | rows = 7 | `expect_table_row_count_to_equal` | gate | pass | 7 |
| `reason_mix.primary_key` | primary key (reason) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.reason.not_null` | reason has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.reason.allowed` | reason only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.contacts.not_null` | contacts has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.contacts.range` | contacts >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.avg_AHT.not_null` | avg_AHT has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.avg_AHT.range` | avg_AHT in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.repeat_rate.not_null` | repeat_rate has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `reason_mix.repeat_rate.range` | repeat_rate in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `savings_summary.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | scenario, agent_hours, cost, SL |
| `savings_summary.rows` | rows = 4 | `expect_table_row_count_to_equal` | gate | pass | 4 |
| `savings_summary.primary_key` | primary key (scenario) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.scenario.not_null` | scenario has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.scenario.allowed` | scenario only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.agent_hours.not_null` | agent_hours has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.agent_hours.range` | agent_hours >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.cost.not_null` | cost has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.cost.range` | cost >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.SL.not_null` | SL has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4 rows unexpected |
| `savings_summary.SL.range` | SL in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `outputs.summary_opt_hours` | summary agent-hours = plan's scheduled agents x 0.5 h (expected 44390.5) | `expect_column_max_to_be_between` | gate | pass | 44390.5 |
| `outputs.summary_peak_scheduled` | summary peak = plan's busiest slot (expected 196) | `expect_column_max_to_be_between` | gate | pass | 196 |
| `outputs.summary_flat_hours` | naive baseline = peak x every slot x 0.5 h (expected 141,120) | `expect_column_max_to_be_between` | gate | pass | 141,120 |
| `outputs.summary_realistic_hours` | realistic baseline = each 8h shift at its busiest slot (expected 78,200) | `expect_column_max_to_be_between` | gate | pass | 78,200 |
| `outputs.summary_opt_cost` | optimized cost = agent-hours x $25 (expected 1109762.5 ± 1e-06) | `expect_column_max_to_be_between` | gate | pass | 1109762.5 |
| `outputs.summary_saved_dollars` | savings vs naive (expected 2418237.5 ± 1e-06) | `expect_column_max_to_be_between` | gate | pass | 2418237.5 |
| `outputs.summary_realistic_saved_dollars` | savings vs realistic (the headline) (expected 845237.5 ± 1e-06) | `expect_column_max_to_be_between` | gate | pass | 845237.5 |
| `outputs.summary_realistic_pct_reduction` | % reduction vs realistic (expected 0.4323465473 ± 1e-12) | `expect_column_max_to_be_between` | gate | pass | 0.4323465473 |
| `outputs.savings_naive_hours` | savings CSV naive agent-hours = summary (expected 141,120 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 141,120 |
| `outputs.savings_realistic_hours` | savings CSV realistic agent-hours = summary (expected 78,200 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 78,200 |
| `outputs.savings_optimized_hours` | savings CSV optimized agent-hours = summary (expected 44390.5 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 44390.5 |
| `outputs.savings_deflection_hours` | savings CSV deflection agent-hours = summary (expected 39732.5 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 39732.5 |
| `outputs.savings_optimized_sl` | savings CSV optimized SL = plan's volume-weighted SL (expected 0.8784 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 0.8784 |
| `outputs.kpi_service_level` | dashboard SL tile = plan's volume-weighted SL (%) (expected 87.84118058 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 87.84118058 |
| `outputs.kpi_asa` | dashboard ASA tile = plan's volume-weighted ASA (expected 7.897988411 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 7.897988411 |
| `outputs.kpi_occupancy` | dashboard occupancy tile = plan's load / required agents (%) (expected 87.18286835 ± 1e-09) | `expect_column_max_to_be_between` | gate | pass | 87.18286835 |
| `outputs.kpi_saved` | dashboard savings tile = summary's savings vs realistic (expected 845237.5 ± 1e-06) | `expect_column_max_to_be_between` | gate | pass | 845237.5 |
| `outputs.reason_total_equals_actuals` | reason-mix contacts = sum of daily actuals (expected 4,740,671) | `expect_column_max_to_be_between` | gate | pass | 4,740,671 |
| `outputs.savings_cost_matches_hours` | every scenario's cost = agent-hours x $25 (to the dollar) | `expect_column_values_to_be_between` | gate | pass | 0 of 4 rows unexpected |
| `outputs.interval_staffing.volume` | interval volume = plan mean by interval, rounded to 1 dp | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `outputs.interval_staffing.required` | interval required = plan mean by interval, rounded to 1 dp | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `outputs.interval_staffing.scheduled` | interval scheduled = plan mean by interval, rounded to 1 dp | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `outputs.interval_staffing.SL` | interval SL = plan mean by interval, rounded to 4 dp | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `outputs.interval_staffing.occupancy` | interval occupancy = plan mean by interval, rounded to 4 dp | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `outputs.daily_volume.first_day` | daily_volume starts on SIM_START (expected 0) | `expect_column_max_to_be_between` | gate | pass | 0 |
| `outputs.daily_volume.last_day` | daily_volume ends on the horizon's last day (expected 394) | `expect_column_max_to_be_between` | gate | pass | 394 |
| `outputs.daily_volume.no_gaps` | daily_volume dates are consecutive | `expect_column_values_to_be_in_set` | gate | pass | 0 of 394 rows unexpected |
| `outputs.daily_volume.forecast` | daily_volume forecast = forecast.csv forecast | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `outputs.daily_volume.forecast_lower` | daily_volume lower = forecast.csv forecast_lower | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `outputs.daily_volume.forecast_upper` | daily_volume upper = forecast.csv forecast_upper | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
| `outputs.daily_volume.actual` | daily_volume actual = forecast.csv actual | `expect_column_values_to_be_between` | gate | pass | 0 of 60 rows unexpected |
