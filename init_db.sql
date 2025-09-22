-- TB Data Warehouse Initialization Script
-- This script sets up the database schema for the TB data processing pipeline

-- Create schema for TB analytics
CREATE SCHEMA IF NOT EXISTS tb_analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA tb_analytics TO postgres;

-- Country summary table with partitioning support
CREATE TABLE IF NOT EXISTS tb_analytics.country_summary (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    iso3 VARCHAR(3) NOT NULL,
    year INTEGER NOT NULL,
    total_cases BIGINT DEFAULT 0,
    new_cases BIGINT DEFAULT 0,
    deaths BIGINT DEFAULT 0,
    prevalence BIGINT DEFAULT 0,
    population BIGINT DEFAULT 0,
    total_cases_per_100k NUMERIC(10,2) DEFAULT 0,
    new_cases_per_100k NUMERIC(10,2) DEFAULT 0,
    deaths_per_100k NUMERIC(10,2) DEFAULT 0,
    case_fatality_rate NUMERIC(5,2) DEFAULT 0,
    new_case_rate NUMERIC(5,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(iso3, year)
);

-- Yearly trends table for regional aggregations
CREATE TABLE IF NOT EXISTS tb_analytics.yearly_trends (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL UNIQUE,
    total_cases_region BIGINT DEFAULT 0,
    new_cases_region BIGINT DEFAULT 0,
    deaths_region BIGINT DEFAULT 0,
    total_population BIGINT DEFAULT 0,
    avg_cases_per_100k NUMERIC(10,2) DEFAULT 0,
    avg_case_fatality_rate NUMERIC(5,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Country trends table for time-series analysis
CREATE TABLE IF NOT EXISTS tb_analytics.country_trends (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    iso3 VARCHAR(3) NOT NULL,
    year INTEGER NOT NULL,
    total_cases BIGINT DEFAULT 0,
    new_cases BIGINT DEFAULT 0,
    deaths BIGINT DEFAULT 0,
    total_cases_per_100k NUMERIC(10,2) DEFAULT 0,
    new_cases_per_100k NUMERIC(10,2) DEFAULT 0,
    deaths_per_100k NUMERIC(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(iso3, year)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_country_summary_iso3_year ON tb_analytics.country_summary(iso3, year);
CREATE INDEX IF NOT EXISTS idx_country_summary_year ON tb_analytics.country_summary(year);
CREATE INDEX IF NOT EXISTS idx_country_summary_country ON tb_analytics.country_summary(country);

CREATE INDEX IF NOT EXISTS idx_country_trends_iso3 ON tb_analytics.country_trends(iso3);
CREATE INDEX IF NOT EXISTS idx_country_trends_year ON tb_analytics.country_trends(year);
CREATE INDEX IF NOT EXISTS idx_country_trends_iso3_year ON tb_analytics.country_trends(iso3, year);

CREATE INDEX IF NOT EXISTS idx_yearly_trends_year ON tb_analytics.yearly_trends(year);

-- Create function for updating timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_country_summary_updated_at 
    BEFORE UPDATE ON tb_analytics.country_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_yearly_trends_updated_at 
    BEFORE UPDATE ON tb_analytics.yearly_trends 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_country_trends_updated_at 
    BEFORE UPDATE ON tb_analytics.country_trends 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create materialized views for faster query performance
CREATE MATERIALIZED VIEW IF NOT EXISTS tb_analytics.latest_country_stats AS
SELECT 
    cs.*,
    RANK() OVER (ORDER BY total_cases DESC) as cases_rank,
    RANK() OVER (ORDER BY total_cases_per_100k DESC) as rate_rank
FROM tb_analytics.country_summary cs
WHERE year = (SELECT MAX(year) FROM tb_analytics.country_summary);

-- Create unique index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_country_stats_iso3 
ON tb_analytics.latest_country_stats(iso3);

-- Regional aggregation view
CREATE MATERIALIZED VIEW IF NOT EXISTS tb_analytics.regional_summary AS
SELECT 
    year,
    COUNT(DISTINCT iso3) as countries_count,
    SUM(total_cases) as total_cases_region,
    SUM(new_cases) as new_cases_region,
    SUM(deaths) as deaths_region,
    SUM(population) as total_population,
    AVG(total_cases_per_100k) as avg_cases_per_100k,
    AVG(case_fatality_rate) as avg_case_fatality_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_cases_per_100k) as median_cases_per_100k
FROM tb_analytics.country_summary
GROUP BY year
ORDER BY year;

-- Create unique index on regional summary
CREATE UNIQUE INDEX IF NOT EXISTS idx_regional_summary_year 
ON tb_analytics.regional_summary(year);

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION tb_analytics.refresh_views()
RETURNS void AS $
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY tb_analytics.latest_country_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY tb_analytics.regional_summary;
END;
$ LANGUAGE plpgsql;

-- Insert sample data for testing (optional)
INSERT INTO tb_analytics.country_summary (country, iso3, year, total_cases, new_cases, deaths, population, total_cases_per_100k, new_cases_per_100k, deaths_per_100k, case_fatality_rate) VALUES
    ('Indonesia', 'IDN', 2023, 969000, 800000, 93000, 275000000, 352.4, 290.9, 33.8, 9.6),
    ('Philippines', 'PHL', 2023, 650000, 550000, 67000, 115000000, 565.2, 478.3, 58.3, 10.3),
    ('Vietnam', 'VNM', 2023, 174000, 150000, 14000, 98000000, 177.6, 153.1, 14.3, 8.0),
    ('Thailand', 'THA', 2023, 92000, 80000, 9100, 72000000, 127.8, 111.1, 12.6, 9.9),
    ('Malaysia', 'MYS', 2023, 25000, 22000, 1600, 34000000, 73.5, 64.7, 4.7, 6.4)
ON CONFLICT (iso3, year) DO NOTHING;

-- Grant all necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tb_analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tb_analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA tb_analytics TO postgres;

-- Create a user for the application (optional)
DO $
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'tb_app_user') THEN
        CREATE USER tb_app_user WITH PASSWORD 'tb_app_pass';
    END IF;
END
$;

GRANT CONNECT ON DATABASE tb_data_warehouse TO tb_app_user;
GRANT USAGE ON SCHEMA tb_analytics TO tb_app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tb_analytics TO tb_app_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA tb_analytics TO tb_app_user;

-- Log the initialization
INSERT INTO tb_analytics.yearly_trends (year, total_cases_region, new_cases_region, deaths_region, total_population, avg_cases_per_100k, avg_case_fatality_rate)
VALUES (2023, 0, 0, 0, 0, 0, 0)
ON CONFLICT (year) DO NOTHING;

-- Create a status table for tracking pipeline runs
CREATE TABLE IF NOT EXISTS tb_analytics.pipeline_status (
    id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status_run_id ON tb_analytics.pipeline_status(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_status_start_time ON tb_analytics.pipeline_status(start_time);

-- Grant permissions on new table
GRANT ALL PRIVILEGES ON tb_analytics.pipeline_status TO postgres;
GRANT SELECT, INSERT, UPDATE ON tb_analytics.pipeline_status TO tb_app_user;

-- Success message
SELECT 'TB Data Warehouse initialized successfully!' as status;