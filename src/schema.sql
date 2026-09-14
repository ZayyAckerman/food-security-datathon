-- Unified SQL view for presentation metrics and Airflow DAGs
--
-- The underlying ETL tables remain separate:
--   country_nutritional_resilience
--   dbm_by_country
--   sa_food_insecurity_by_province
--
-- DBM survey observations are preserved in dbm_by_country.
-- This view aggregates them to one row per country for presentation.

CREATE OR REPLACE VIEW view_eat_trade_empowerment_matrix AS

WITH dbm_country_summary AS (

    SELECT
        country_code,
        country,

        AVG(dbm_pct) AS double_burden_malnutrition_pct,

        CASE
            WHEN AVG(dbm_pct) > 10 THEN 'high'
            WHEN AVG(dbm_pct) >= 6.7 THEN 'moderate'
            ELSE 'low'
        END AS dbm_risk_tier

    FROM dbm_by_country

    GROUP BY
        country_code,
        country
)

SELECT

    r.country,
    r.country_code,

    d.double_burden_malnutrition_pct,
    d.dbm_risk_tier,

    r.female_ag_decision_score,
    r.crop_diversity_index,
    r.dietary_diversity_score,

    r.net_staple_import_dependency_pct,
    r.food_price_volatility_index,

    r.nutritional_resilience_index

FROM country_nutritional_resilience AS r

INNER JOIN dbm_country_summary AS d
    ON r.country_code = d.country_code;