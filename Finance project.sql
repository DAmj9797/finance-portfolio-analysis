/* ============================================================
   🚀 FINANCIAL ANALYTICS
   End-to-End: Data Cleaning → Metrics → Risk → Decision Engine
   ============================================================ */


-- ============================================================
-- 1️ DATABASE SETUP
-- ============================================================

IF DB_ID('Finance_Project') IS NULL
    CREATE DATABASE Finance_Project;
GO

USE Finance_Project;
GO


-- ============================================================
-- 2️ CLEAN + NORMALIZE PRICE DATA (WIDE → LONG)
-- ============================================================

IF OBJECT_ID('clean_fact_prices', 'U') IS NOT NULL
    DROP TABLE clean_fact_prices;

SELECT 
    CAST(Date AS DATE) AS Date,
    stock_name,
    price
INTO clean_fact_prices
FROM (
    SELECT Date, 'RELIANCE.NS' AS stock_name, [RELIANCE.NS] AS price FROM raw_financial_data
    UNION ALL SELECT Date, 'TCS.NS', [TCS.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'HDFCBANK.NS', [HDFCBANK.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'INFY.NS', [INFY.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'ICICIBANK.NS', [ICICIBANK.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'ITC.NS', [ITC.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'SBIN.NS', [SBIN.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'AXISBANK.NS', [AXISBANK.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'KOTAKBANK.NS', [KOTAKBANK.NS] FROM raw_financial_data
    UNION ALL SELECT Date, 'NIFTY50', [NIFTY50] FROM raw_financial_data
) t
WHERE price IS NOT NULL
  AND price > 0;  -- remove invalid prices


-- ============================================================
-- 3️ DAILY RETURNS (CORE FINANCIAL METRIC)
-- ============================================================

IF OBJECT_ID('clean_fact_returns', 'U') IS NOT NULL
    DROP TABLE clean_fact_returns;

SELECT 
    Date,
    stock_name,
    price,
    LAG(price) OVER (PARTITION BY stock_name ORDER BY Date) AS prev_price,

    -- % change (return)
    (price - LAG(price) OVER (PARTITION BY stock_name ORDER BY Date)) * 1.0
    / LAG(price) OVER (PARTITION BY stock_name ORDER BY Date) AS daily_return

INTO clean_fact_returns
FROM clean_fact_prices;

-- remove invalid extreme returns (data noise)
DELETE FROM clean_fact_returns
WHERE daily_return IS NULL
   OR daily_return > 1
   OR daily_return < -1;


-- ============================================================
-- 4️ VOLATILITY (RISK MEASUREMENT)
-- ============================================================

IF OBJECT_ID('stock_volatility', 'U') IS NOT NULL
    DROP TABLE stock_volatility;

SELECT 
    stock_name,
    STDEV(daily_return) AS volatility   -- standard deviation = risk
INTO stock_volatility
FROM clean_fact_returns
GROUP BY stock_name;


-- ============================================================
-- 5️ MARKET BENCHMARK (NIFTY)
-- ============================================================

IF OBJECT_ID('nifty_returns', 'U') IS NOT NULL
    DROP TABLE nifty_returns;

SELECT *
INTO nifty_returns
FROM clean_fact_returns
WHERE stock_name = 'NIFTY50';


-- ============================================================
-- 6️ ALPHA (STOCK VS MARKET PERFORMANCE)
-- ============================================================

IF OBJECT_ID('stock_vs_market', 'U') IS NOT NULL
    DROP TABLE stock_vs_market;

SELECT 
    f.Date,
    f.stock_name,
    f.daily_return,
    n.daily_return AS market_return,

    -- excess return over market
    (f.daily_return - n.daily_return) AS alpha

INTO stock_vs_market
FROM clean_fact_returns f
JOIN nifty_returns n
    ON f.Date = n.Date
WHERE f.stock_name != 'NIFTY50';


-- ============================================================
-- 7️ STOCK PERFORMANCE (AVERAGE ALPHA)
-- ============================================================

IF OBJECT_ID('stock_performance', 'U') IS NOT NULL
    DROP TABLE stock_performance;

SELECT 
    stock_name,
    AVG(alpha) AS avg_alpha   -- average outperformance
INTO stock_performance
FROM stock_vs_market
GROUP BY stock_name;


-- ============================================================
-- 8️ SHARPE RATIO (RISK-ADJUSTED RETURN)
-- ============================================================

IF OBJECT_ID('stock_sharpe', 'U') IS NOT NULL
    DROP TABLE stock_sharpe;

SELECT 
    p.stock_name,
    p.avg_alpha / NULLIF(v.volatility, 0) AS sharpe_ratio  -- return per unit risk
INTO stock_sharpe
FROM stock_performance p
JOIN stock_volatility v
    ON p.stock_name = v.stock_name;


-- ============================================================
-- 9️ MAX DRAWDOWN (DOWNSIDE RISK)
-- ============================================================

IF OBJECT_ID('stock_drawdown', 'U') IS NOT NULL
    DROP TABLE stock_drawdown;

SELECT 
    stock_name,
    MIN(daily_return) AS max_drawdown   -- worst single-day loss proxy
INTO stock_drawdown
FROM clean_fact_returns
GROUP BY stock_name;


-- ============================================================
-- 10 COMPOSITE SCORING MODEL 
-- ============================================================

IF OBJECT_ID('stock_scoring', 'U') IS NOT NULL
    DROP TABLE stock_scoring;

SELECT 
    p.stock_name,
    p.avg_alpha,
    v.volatility,
    s.sharpe_ratio,
    d.max_drawdown,

    -- weighted decision model (return + risk + downside)
    (p.avg_alpha * 0.5) 
  + (s.sharpe_ratio * 0.3)
  - (v.volatility * 0.1)
  - (ABS(d.max_drawdown) * 0.1) AS investment_score

INTO stock_scoring
FROM stock_performance p
JOIN stock_volatility v ON p.stock_name = v.stock_name
JOIN stock_sharpe s ON p.stock_name = s.stock_name
JOIN stock_drawdown d ON p.stock_name = d.stock_name;


-- ============================================================
--  FINAL DECISION ENGINE
-- ============================================================

SELECT 
    stock_name,
    ROUND(investment_score, 6) AS score,

    CASE 
        WHEN investment_score > 0.02 THEN 'STRONG BUY'
        WHEN investment_score > 0.01 THEN 'BUY'
        WHEN investment_score > 0 THEN 'HOLD'
        ELSE 'SELL'
    END AS decision

FROM stock_scoring
ORDER BY score DESC;


-- ============================================================
-- 🧹 CLEANUP UNUSED TABLES
-- ============================================================

DROP TABLE IF EXISTS stock_signals;
DROP TABLE IF EXISTS stock_crossover_signals;
DROP TABLE IF EXISTS stock_moving_avg;
DROP TABLE IF EXISTS stock_sector;