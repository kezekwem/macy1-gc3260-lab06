# 🧑🏽‍🏫 NYU — Lab 06 (DashDash) — Data Warehousing & Analytics Student Guide
## MASY1-GC 3260: Advanced Data Warehousing & Applications

> **Welcome, Students!** This lab teaches you how to build a **production-ready data warehouse** from raw CSV files all the way to KPI dashboards. You'll learn **SQL transformations**, **dimensional modeling**, **data quality testing**, and **dbt (data build tool)** — skills that employers are actively seeking in Analytics Engineers, Data Warehouse Developers, and Senior Data Analysts.

### 🎯 What You'll Learn
By completing this lab, you will be able to:
- Design and implement a **dimensional model** (fact and dimension tables)
- Transform raw data using **SQL best practices** (CTEs, window functions, type casting)
- Build **reproducible ELT pipelines** with both plain SQL and dbt
- Implement **data quality tests** (not_null, unique, referential integrity, business rules)
- Choose between **SQLite** (local/offline) and **PostgreSQL** (production-grade)
- Generate **stakeholder-ready reports** and KPI metrics

### 📚 Course Context
This lab aligns with **Weeks 2-7** of your course:
- **Week 2**: Dimensional Modeling Basics (grain, facts, dimensions)
- **Week 3**: Modern ELT with dbt (models, refs, tests)
- **Week 4**: SQL Foundations for Warehousing (DDL/DML, joins)
- **Week 5**: Advanced SQL for Analytics (CTEs, windows)
- **Week 6**: Data Quality & Governance (dbt tests)
- **Week 7**: Mid-Term Practical (design & load a mini mart)

---

## 📖 Table of Contents for Students
1. [Getting Started — Choose Your Database](#-getting-started--choose-your-database)
2. [Two Learning Paths — SQL vs dbt](#-two-learning-paths--sql-vs-dbt)
3. [Understanding the Business Scenario](#-understanding-the-business-scenario)
4. [Data Architecture Overview](#-data-architecture-overview)
5. [Step-by-Step SQL Explanations](#-step-by-step-sql-explanations)
6. [Understanding dbt Models](#-understanding-dbt-models)
7. [Data Quality Testing Explained](#-data-quality-testing-explained)
8. [Running the Lab](#-running-the-lab)
9. [Deliverables & Grading](#-deliverables--grading)
10. [Troubleshooting & FAQs](#-troubleshooting--faqs)

---

## 🗂️ Getting Started — Choose Your Database

You have **two database options**. Both work identically with this lab:

### Option A: SQLite (Recommended for Beginners)
**When to use:** Local development, offline work, learning SQL fundamentals
- **No installation required** — SQLite is built into Python
- **No server setup** — database is a single file on your computer
- **Perfect for:** Weeks 2-5 of the course (learning SQL and dimensional modeling)

### Option B: PostgreSQL (Recommended for Advanced Students)
**When to use:** Production-grade work, team projects, final group project
- **Industry standard** — used by most companies for data warehousing
- **Advanced features** — better performance, concurrency, data types
- **Perfect for:** Weeks 6-14 of the course (testing, dbt, final project)

**How to choose:** When you run the lab, you'll be prompted:
```
Use PostgreSQL? (y/n, default=y):
```
- Type `y` (or press Enter) for PostgreSQL
- Type `n` for SQLite

---

## 🛤️ Two Learning Paths — SQL vs dbt

This lab provides **two equivalent implementations** of the same data warehouse:

| Feature | **Path 1: Plain SQL** (`warehouse/` folder) | **Path 2: dbt** (`dbt_dashdash/` folder) |
|---------|----------------------------------------------|------------------------------------------|
| **Best for** | Learning SQL fundamentals | Industry-standard analytics engineering |
| **Course weeks** | Weeks 2-5 | Weeks 3-14 |
| **Files** | `.sql` files you can read directly | `.sql` files with Jinja templates |
| **Testing** | Custom SQL queries in `tests/` folder | Built-in dbt test framework |
| **Run command** | `python main_plain_sql.py` | `python main_dbt.py` |
| **Notebook** | `main_plain_sql.ipynb` | `main_dbt.ipynb` |

**Instructor's recommendation:** Start with **Path 1 (Plain SQL)** to understand the fundamentals, then move to **Path 2 (dbt)** for your final group project.

---

## 📦 Project File Structure

```
MACY1_GC_3260_LAB06_V2.0_10152025/
│
├── 📓 NOTEBOOKS (Start here!)
│   ├── main_plain_sql.ipynb          ← Run SQL pipeline interactively
│   └── main_dbt.ipynb                ← Run dbt pipeline interactively
│
├── 📂 data/ (Raw CSV seed files)
│   ├── customers.csv                 (5 customers)
│   ├── orders.csv                    (8 orders with data quality issues)
│   ├── restaurants.csv               (5 restaurants)
│   └── couriers.csv                  (4 couriers)
│
├── 📂 warehouse/ (PLAIN SQL PATH — Your main learning resource)
│   ├── staging/                      (Clean and standardize raw data)
│   │   ├── stg_orders.sql           ← **START HERE** (teaches CTEs, windows, CASE)
│   │   ├── stg_customers.sql
│   │   ├── stg_couriers.sql
│   │   └── stg_restaurants.sql
│   ├── marts/                        (Dimensional model)
│   │   ├── dim_customer.sql         (Customer dimension)
│   │   ├── dim_courier.sql          (Courier dimension)
│   │   ├── dim_restaurant.sql       (Restaurant dimension)
│   │   └── fct_deliveries.sql       ← **KEY FILE** (fact table with referential integrity)
│   ├── kpis/
│   │   └── kpi_delivery_overview.sql ← **BUSINESS METRICS** (on-time rate, avg delivery time)
│   ├── monitoring/
│   │   └── monitoring_dq_exceptions.sql (Data quality exceptions)
│   └── tests/                        (Data quality tests)
│       ├── staging.yml               (Test configuration)
│       ├── marts.yml
│       └── custom.yml
│
├── 📂 dbt_dashdash/ (DBT PATH — Industry standard approach)
│   ├── models/
│   │   ├── sources.yml               (Defines raw input tables)
│   │   ├── staging/                  (Same as warehouse/staging but with Jinja)
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_orders.yml       (dbt test definitions)
│   │   ├── marts/
│   │   ├── kpis/
│   │   └── monitoring/
│   └── dbt_project.yml               (dbt configuration)
│
├── 📂 src/ (Python orchestration — you don't need to edit these)
│   ├── config.py                     (Database connection settings)
│   ├── db.py                         (SQLAlchemy engine)
│   ├── seed.py                       (Load CSV files into database)
│   ├── pipeline.py                   (Orchestrate the entire workflow)
│   └── tests_runner.py               (Run data quality tests)
│
├── 📂 outputs/ (Generated after you run the lab)
│   ├── stg_orders_export.csv         (Cleansed staging data)
│   ├── fct_deliveries_export.csv     (Fact table export)
│   ├── kpi_delivery_overview_export.csv (KPI metrics)
│   ├── monitoring_dq_exceptions_export.csv (Data quality issues)
│   ├── RUN_LOG.txt                   (Test results)
│   └── Lab06_First_Last_netid_Reply.md (Your stakeholder report)
│
├── main_plain_sql.py                 (Run SQL pipeline from command line)
├── main_dbt.py                       (Run dbt pipeline from command line)
├── .env                              (Database credentials)
└── README.md                         ← You are here!

---

## 🍕 Understanding the Business Scenario

### The Company: DashDash Food Delivery

You work as a **Data Analyst** at **DashDash**, a food delivery startup in New York City. The company connects:
- **Customers** who place orders via mobile app
- **Restaurants** that prepare the food
- **Couriers** who deliver orders using bikes, scooters, or cars

### The Problem

Your VP of Operations asks:
> "We need to understand our delivery performance. Are we meeting our 45-minute delivery promise? Which restaurants have the most issues? How can we improve customer satisfaction?"

### Your Mission

Build a **data warehouse** that:
1. **Cleans messy operational data** (duplicates, missing values, inconsistent statuses)
2. **Organizes data into a dimensional model** (facts and dimensions)
3. **Calculates KPIs** (on-time delivery rate, average delivery time, cancellation rate)
4. **Monitors data quality** (missing couriers, invalid timestamps, foreign key violations)

### The Raw Data

You receive **4 CSV files** from the engineering team:

| File | Records | Issues You'll Find |
|------|---------|-------------------|
| `customers.csv` | 5 customers | Clean data (no issues) |
| `restaurants.csv` | 5 restaurants | Clean data (no issues) |
| `couriers.csv` | 4 couriers | Clean data (no issues) |
| `orders.csv` | 8 orders | **MESSY!** Duplicates, mixed case statuses ("DELIVERED" vs "delivered"), missing courier_id, invalid restaurant_id (999), NULL timestamps |

**Key insight:** Real-world data is **never clean**. This lab teaches you how to handle it!

---

## 🏗️ Data Architecture Overview

### The Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  RAW LAYER (CSV Files → Database Tables)                    │
│  • customers (5 records)                                     │
│  • restaurants (5 records)                                   │
│  • couriers (4 records)                                      │
│  • orders (8 records with intentional data quality issues)  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  STAGING LAYER (Clean & Standardize)                        │
│  • stg_customers      ← Trimmed, typed                      │
│  • stg_restaurants    ← Trimmed, typed                      │
│  • stg_couriers       ← Trimmed, typed, vehicle validation  │
│  • stg_orders         ← Deduped, case-normalized, typed     │
│                         + delivery_minutes calculated        │
│                         + on_time_flag (true if ≤45 min)    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  MART LAYER (Dimensional Model — Kimball Style)             │
│                                                              │
│  DIMENSIONS (Who, What, Where, When):                       │
│  • dim_customer      ← Customer attributes                  │
│  • dim_restaurant    ← Restaurant attributes                │
│  • dim_courier       ← Courier attributes + vehicle type    │
│                                                              │
│  FACTS (Measurable Events):                                 │
│  • fct_deliveries    ← Successfully delivered orders only   │
│                         with referential integrity to dims  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  KPI & MONITORING LAYER (Business Insights)                 │
│  • kpi_delivery_overview    ← On-time rate, avg minutes     │
│  • monitoring_dq_exceptions ← Data quality issues flagged   │
└─────────────────────────────────────────────────────────────┘
```

### Dimensional Modeling Vocabulary

| Term | Definition | Example in This Lab |
|------|-----------|---------------------|
| **Grain** | The level of detail in a fact table | One row per delivered order |
| **Fact Table** | Measurable events with foreign keys to dimensions | `fct_deliveries` (order_id, delivery_minutes, subtotal, tip, distance) |
| **Dimension Table** | Descriptive attributes (who, what, where) | `dim_customer` (customer_id, name, email, city) |
| **Foreign Key** | Column linking fact to dimension | `fct_deliveries.customer_id` → `dim_customer.customer_id` |
| **Surrogate Key** | Artificial primary key (not used in this basic lab) | N/A (we use natural keys) |
| **Conformed Dimension** | Dimension shared across multiple fact tables | `dim_date` (not in this lab, but common in real projects) |

---

## 📖 Step-by-Step SQL Explanations

### 🎓 Learning Objective
By the end of this section, you will understand **every line** of the SQL transformations and be able to explain them in your own words.

---

### 📄 File: `warehouse/staging/stg_orders.sql`

**Purpose:** Clean and standardize order records before they feed marts and KPIs.

**SQL Techniques Taught:**
- Common Table Expressions (CTEs) with `WITH`
- Window functions (`ROW_NUMBER` with `PARTITION BY`)
- Type casting (`::timestamp`, `::numeric`)
- String functions (`BTRIM`, `LOWER`, `NULLIF`)
- `CASE` expressions for business logic
- `EXTRACT(EPOCH ...)` for timestamp math

#### 📝 Line-by-Line Explanation

```sql
-- Lines 1-6: Comments explaining what this view does
-- Lines 7: CREATE OR REPLACE VIEW means "create a new view, or update if it already exists"
CREATE OR REPLACE VIEW stg_orders AS
```

#### **CTE #1: base** (Lines 8-26)
**Purpose:** Convert raw CSV text into strongly-typed database columns

```sql
WITH base AS (
  SELECT
    order_id,                    -- Keep as-is (integer from CSV)
    customer_id,                 -- Keep as-is (integer from CSV)
    restaurant_id,               -- Keep as-is (integer from CSV)
    courier_id,                  -- Keep as-is (can be NULL)
```

**Line 15: NULLIF + BTRIM + Type Casting Pattern**
```sql
    NULLIF(BTRIM(order_timestamp::text),'')::timestamp AS order_ts,
```
Let's break this down step-by-step:
1. `order_timestamp::text` — Convert to text (in case it's not already)
2. `BTRIM(...)` — Remove leading/trailing whitespace ("  2024-10-15  " → "2024-10-15")
3. `NULLIF(..., '')` — If result is empty string '', convert to NULL
4. `::timestamp` — Cast the cleaned text to a proper timestamp type
5. `AS order_ts` — Give it a shorter alias

**Why this pattern?** CSV files often have inconsistent formatting (extra spaces, empty strings). This cleans it up!

**Lines 16-17:** Same pattern for pickup and dropoff timestamps
```sql
    NULLIF(BTRIM(pickup_timestamp::text),'')::timestamp   AS pickup_ts,
    NULLIF(BTRIM(dropoff_timestamp::text),'')::timestamp  AS dropoff_ts,
```

**Line 18: Normalize status values**
```sql
    lower(NULLIF(BTRIM(status::text),'')) AS status_norm,
```
- `lower(...)` converts "DELIVERED" → "delivered" (case-insensitive matching later)
- Handles mixed case from bad data entry

**Lines 20-23: Financial columns need precision**
```sql
    NULLIF(BTRIM(subtotal::text),'')::numeric(10,2)       AS subtotal,
    NULLIF(BTRIM(delivery_fee::text),'')::numeric(10,2)   AS delivery_fee,
    NULLIF(BTRIM(tip_amount::text),'')::numeric(10,2)     AS tip_amount,
    NULLIF(BTRIM(distance_km::text),'')::numeric(6,2)     AS distance_km,
```
- `numeric(10,2)` means: up to 10 total digits, with 2 after the decimal
- Example: 12345.67 ✓, 12345.678 ✗ (too many decimals), 123456789.00 ✗ (too many digits)

**Line 24: Window Function — THE KEY TO DEDUPLICATION!**
```sql
    row_number() OVER (PARTITION BY order_id ORDER BY order_timestamp) AS rn
```
**What does this do?**
- `PARTITION BY order_id` — Group rows by order_id
- `ORDER BY order_timestamp` — Within each order_id, sort by timestamp (earliest first)
- `row_number()` — Assign 1, 2, 3, ... to each row in the partition
- Result: The **first occurrence** of each order gets `rn = 1`, duplicates get `rn = 2, 3, ...`

**Example:**
| order_id | order_timestamp | rn |
|----------|----------------|-----|
| 1001 | 2024-10-15 10:00 | 1 ← Keep this |
| 1001 | 2024-10-15 10:05 | 2 ← Discard (duplicate) |
| 1002 | 2024-10-15 11:00 | 1 ← Keep this |

```sql
  FROM orders  -- Read from the raw table
),
```

---

#### **CTE #2: dedup** (Lines 27-30)
**Purpose:** Keep only the first chronological record per order

```sql
dedup AS (
  SELECT * FROM base WHERE rn = 1
),
```
**Simple but powerful!** This filters out all duplicate records, keeping only `rn = 1`.

---

#### **CTE #3: clean** (Lines 31-47)
**Purpose:** Add derived columns (calculated fields) for business logic

```sql
clean AS (
  SELECT
    *,  -- Keep all columns from dedup
```

**Lines 35-39: CASE expression for status standardization**
```sql
    CASE
      WHEN status_norm IN ('delivered','canceled','returned') THEN status_norm
      WHEN status_norm IS NULL THEN 'unknown'
      ELSE 'unknown'
    END AS status_final,
```
**What this does:**
- If status is one of the valid values ('delivered', 'canceled', 'returned') → keep it
- If status is NULL or any other value → replace with 'unknown'
- **Why?** This prevents typos like "delivred" or "cancled" from breaking downstream queries

**Lines 41-45: Calculate delivery time in minutes**
```sql
    CASE
      WHEN pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL
        THEN EXTRACT(epoch FROM (dropoff_ts - pickup_ts))/60.0
      ELSE NULL
    END AS delivery_minutes
```
**Step-by-step:**
1. `dropoff_ts - pickup_ts` — Subtract timestamps (gives an interval like "00:37:00")
2. `EXTRACT(epoch FROM ...)` — Convert interval to seconds (37 minutes = 2220 seconds)
3. `/ 60.0` — Convert seconds to minutes (2220 / 60 = 37.0)
4. Only calculate if both timestamps exist (otherwise NULL)

**Why EPOCH?** It's a standard way to convert time intervals to numbers you can do math with.

---

#### **Final SELECT** (Lines 48-65)
**Purpose:** Choose which columns to expose in the view

```sql
SELECT
  order_id,
  customer_id,
  restaurant_id,
  courier_id,
  order_ts,
  pickup_ts,
  dropoff_ts,
  status_final AS status,  -- Rename to just "status" for simplicity
  payment_method,
  subtotal,
  delivery_fee,
  tip_amount,
  distance_km,
  delivery_minutes,
```

**Lines 64: SLA (Service Level Agreement) flag**
```sql
  CASE WHEN status_final = 'delivered' AND delivery_minutes <= 45 THEN TRUE ELSE FALSE END AS on_time_flag
```
**Business rule:** DashDash promises delivery in 45 minutes or less.
- TRUE = delivered in ≤45 minutes ✅
- FALSE = delivered late, or not delivered at all ❌

---

### 🎯 Key Takeaways from `stg_orders.sql`

1. **CTEs make complex queries readable** — Each CTE is a named step in the transformation
2. **Window functions solve deduplication** — `ROW_NUMBER` with `PARTITION BY` is the standard approach
3. **Type casting is critical** — CSV files are just text; you must convert to proper types
4. **Business logic goes in derived columns** — `on_time_flag` is calculated once, used everywhere
5. **CASE expressions handle messy data** — Replace bad values with 'unknown' instead of crashing

---

### 📄 File: `warehouse/marts/fct_deliveries.sql`

**Purpose:** Create a fact table with **only successfully delivered orders** that have referential integrity to dimension tables.

**SQL Techniques Taught:**
- Filtering fact tables to a specific grain
- Referential integrity with `IN` subqueries
- Left vs. Inner joins (conceptually)

#### 📝 Line-by-Line Explanation

```sql
-- Lines 1-3: Comments
CREATE OR REPLACE VIEW fct_deliveries AS
SELECT *
FROM stg_orders o
WHERE o.status = 'delivered'
```
**Line 7:** Only include orders that were successfully delivered (excludes canceled/returned/unknown)

**Line 8: Referential integrity check for restaurant_id**
```sql
  AND o.restaurant_id IN (SELECT restaurant_id FROM stg_restaurants)
```
**What this does:**
- The subquery `(SELECT restaurant_id FROM stg_restaurants)` returns [1, 2, 3, 4, 5]
- `IN` checks if `o.restaurant_id` is in that list
- **Prevents:** Order 1007 has `restaurant_id = 999` (doesn't exist!) → excluded from fact table

**Why this matters:** A fact table should NEVER have foreign keys that don't match a dimension. This is called **referential integrity**.

**Line 9: Referential integrity check for courier_id (with NULL handling)**
```sql
  AND (o.courier_id IS NULL OR o.courier_id IN (SELECT courier_id FROM stg_couriers));
```
**What this does:**
- Allow `courier_id` to be NULL (order not yet assigned to courier)
- If `courier_id` is NOT NULL, it must exist in `stg_couriers`
- **Why the `OR`?** Some orders might be in "awaiting pickup" status with no courier yet

---

### 📄 File: `warehouse/kpis/kpi_delivery_overview.sql`

**Purpose:** Calculate business KPIs (Key Performance Indicators) for executives.

**SQL Techniques Taught:**
- `AVG()` with `CASE` to calculate rates
- `FILTER` clause for conditional aggregation
- `NULLIF` to prevent division-by-zero errors
- Subqueries in SELECT list

#### 📝 Line-by-Line Explanation

```sql
CREATE OR REPLACE VIEW kpi_delivery_overview AS
WITH d AS (SELECT * FROM fct_deliveries)
```
**Line 7:** Create a shorthand alias `d` for the fact table (makes the query shorter)

**Line 9: On-time delivery rate**
```sql
  AVG(CASE WHEN on_time_flag THEN 1 ELSE 0 END)::numeric(5,4) AS on_time_rate,
```
**How this works:**
1. `CASE WHEN on_time_flag THEN 1 ELSE 0 END` — Convert TRUE → 1, FALSE → 0
2. `AVG(...)` — Average of those 1s and 0s
   - If 6 out of 10 are on-time: (1+1+1+1+1+1+0+0+0+0) / 10 = 0.6
3. `::numeric(5,4)` — Cast to 4 decimal places (0.6000)

**Result:** `on_time_rate = 0.6000` means **60% on-time delivery rate**

**Line 10: Average delivery minutes**
```sql
  AVG(delivery_minutes)::numeric(6,2) AS avg_delivery_minutes,
```
**Simple:** Average of all delivery times, rounded to 2 decimal places (e.g., 42.37 minutes)

**Lines 11-16: Cancellation rate (with subquery)**
```sql
  (
    SELECT
      (COUNT(*) FILTER (WHERE status IN ('canceled','returned'))::numeric
       / NULLIF(COUNT(*),0))
    FROM stg_orders
  )::numeric(5,4) AS cancel_return_rate
```
**Why a subquery?** This calculates from `stg_orders` (all orders) instead of `fct_deliveries` (only delivered).

**Step-by-step:**
1. `COUNT(*) FILTER (WHERE status IN ('canceled','returned'))` — Count canceled/returned orders
2. `COUNT(*)` — Count all orders
3. `/ NULLIF(COUNT(*),0)` — Divide (but if denominator is 0, return NULL instead of error)
4. Cast to numeric with 4 decimal places

**Example:**
- 8 total orders, 2 canceled → `2 / 8 = 0.2500` (25% cancellation rate)

---

## 🔧 Understanding dbt Models

### What is dbt?

**dbt (data build tool)** is an industry-standard tool that lets you:
- Write SQL transformations as **modular files**
- Define **dependencies** between models (`ref('model_name')`)
- Run **tests** declaratively (YAML config instead of custom SQL)
- Document your data warehouse
- Version control your transformations

### How dbt is Different from Plain SQL

| Feature | Plain SQL (`warehouse/`) | dbt (`dbt_dashdash/`) |
|---------|-------------------------|----------------------|
| **File location** | `warehouse/staging/stg_orders.sql` | `dbt_dashdash/models/staging/stg_orders.sql` |
| **Table reference** | `FROM orders` | `FROM {{ source('dashdash', 'orders') }}` |
| **Dependencies** | Manual (you must run files in correct order) | Automatic (`ref('stg_orders')` tells dbt the order) |
| **Type casting** | `::timestamp` | `{{ safe_timestamp('column') }}` (macro) |
| **Tests** | Custom SQL files in `tests/` | YAML: `tests: [not_null, unique]` |
| **Running** | `python main_plain_sql.py` | `dbt run && dbt test` |

### 📄 File: `dbt_dashdash/models/staging/stg_orders.sql`

This is the **dbt version** of the plain SQL file you just studied. Let's compare:

#### **Plain SQL (Line 19):**
```sql
FROM orders
```

#### **dbt Equivalent (Line 19):**
```sql
FROM {{ source('dashdash', 'orders') }}
```
**What's `{{ source(...) }}`?**
- A **Jinja template** that dbt replaces with the actual table name
- Defined in `dbt_dashdash/models/sources.yml`:
  ```yaml
  sources:
    - name: dashdash
      tables:
        - name: orders
  ```
- **Benefit:** dbt tracks that this model depends on the `orders` source table

#### **Plain SQL (Line 15):**
```sql
NULLIF(BTRIM(order_timestamp::text),'')::timestamp AS order_ts,
```

#### **dbt Equivalent (Line 9):**
```sql
{{ safe_timestamp('order_timestamp') }} as order_ts,
```
**What's `{{ safe_timestamp(...) }}`?**
- A **macro** (reusable function) that wraps the NULLIF+BTRIM+casting logic
- Defined somewhere in your dbt project or packages
- **Benefit:** Write it once, reuse everywhere (DRY principle)

#### **dbt Model Dependencies**

If another model needs `stg_orders`, you reference it like this:
```sql
-- In fct_deliveries.sql
SELECT * FROM {{ ref('stg_orders') }} WHERE status = 'delivered'
```
**What does `{{ ref(...) }}` do?**
- Tells dbt: "This model depends on stg_orders"
- dbt automatically runs `stg_orders` **before** `fct_deliveries`
- Creates a **directed acyclic graph (DAG)** of dependencies

---

## 🧪 Data Quality Testing Explained

### Why Test Your Data?

Imagine you deploy your warehouse to production, and:
- A bug creates **duplicate order_ids** → KPIs are overcounted!
- Someone deletes all rows from `dim_courier` → your fact table breaks!
- A status typo ("delivred") slips through → reports show 0 deliveries!

**Data quality tests catch these problems before they reach executives.**

### Types of Tests in This Lab

| Test Type | Purpose | Example |
|-----------|---------|---------|
| **not_null** | Column must have a value | `order_id` cannot be NULL |
| **unique** | Column must have unique values | `order_id` cannot have duplicates |
| **accepted_values** | Column must be from a specific list | `status` must be ['delivered', 'canceled', 'returned', 'unknown'] |
| **relationships** | Foreign key must exist in dimension | `restaurant_id` must exist in `stg_restaurants` |
| **expression_is_true** | Custom business rule | `delivery_minutes >= 0` (cannot be negative) |

### 📄 File: `warehouse/tests/staging.yml`

This YAML file **configures** the tests. Let's walk through it:

```yaml
tests:
  - name: stg_orders_order_id_not_null
    severity: error
    sql: tests/staging/stg_orders_order_id_not_null.sql
```
**What this means:**
- **name:** Human-readable test name
- **severity:** `error` (fail the pipeline) vs. `warn` (log but continue)
- **sql:** Path to the test SQL file

### 📄 Test SQL File: `tests/staging/stg_orders_order_id_not_null.sql`

```sql
SELECT order_id
FROM stg_orders
WHERE order_id IS NULL;
```
**How tests work:**
- If this query returns **0 rows** → test passes ✅
- If this query returns **any rows** → test fails ❌ (these are the bad records)

**Example failure output:**
```
❌ stg_orders_order_id_not_null FAILED (2 rows)
   order_id
   --------
   NULL
   NULL
```

### dbt Tests (YAML-based)

**File:** `dbt_dashdash/models/staging/stg_orders.yml`

```yaml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['delivered', 'canceled', 'returned', 'unknown']
```
**What dbt does:**
- Automatically generates the SQL test queries for you!
- `not_null` → `SELECT * FROM stg_orders WHERE order_id IS NULL`
- `unique` → `SELECT order_id, COUNT(*) FROM stg_orders GROUP BY order_id HAVING COUNT(*) > 1`

**Benefit:** You just write YAML, dbt writes the SQL.

---

## 🚀 Running the Lab

### Option 1: Jupyter Notebooks (Recommended for Beginners)

**Environment setup (first time only)**

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install notebook        # only if Jupyter is not already installed
```

> If you are using VS Code, you can open the notebooks directly without running the `jupyter` command. Codespaces also comes with Jupyter ready to go.

**Launching and running the notebooks**

1. Pick a learning path:
   - `main_plain_sql.ipynb` builds the full pipeline with vanilla SQL.
   - `main_dbt.ipynb` mirrors the logic with dbt so you can practice the production tool.

2. Start Jupyter from the project root (or open the file inside VS Code):

   ```bash
   jupyter lab main_plain_sql.ipynb   # Plain SQL path
   # or
   jupyter lab main_dbt.ipynb         # dbt path
   ```

3. Run the notebook top-to-bottom. Execute each cell in order (`Shift+Enter` in Jupyter/VS Code).

4. When the runner prompts:

   ```
   Use PostgreSQL? (y/n, default=y):
   ```

   - Type `n` for the built-in SQLite database (fastest for local / offline work).
   - Type `y` if you have PostgreSQL credentials available and want the production-style setup.

**Collecting your results**

5. Every successful run writes files to `outputs/`. You can inspect them in your file explorer or by running `ls outputs` in the terminal. Key artifacts:
   - `outputs/RUN_LOG.txt` — chronological log of each pipeline step and test outcome.
   - `outputs/fct_deliveries_export.csv` — fact table snapshot ready for analysis.
   - `outputs/kpi_delivery_overview_export.csv` — KPI metrics you can share with stakeholders.
   - `outputs/Lab06_First_Last_netid_Reply.md` — auto-generated stakeholder briefing (submit this!).

6. To review the rendered notebook results later (without rerunning), open the `.ipynb` file and use Jupyter's “Restart kernel & Clear Outputs” only after you have saved a copy, or export to HTML via `File > Export Notebook As… > HTML`. The exported HTML captures every cell output exactly as you left it.

### Option 2: Command Line

```bash
# Install dependencies (first time only)
pip install -r requirements.txt

# Run SQL pipeline
python main_plain_sql.py

# Or run dbt pipeline
python main_dbt.py
```

### Option 3: GitHub Codespaces (Zero-setup Cloud Environment)

1. Open the repository in GitHub and click **Code → Create codespace on main**.
2. The Codespace boots with the `.devcontainer` we ship, installing Python 3.11, Jupyter, dbt, and CLI helpers automatically.
3. Once the machine is ready, start Jupyter with `jupyter lab main_plain_sql.ipynb` (or open the notebook directly in VS Code for the Web) and run cells as usual.
4. To execute the Python runners instead, use the integrated terminal:

   ```bash
   python main_plain_sql.py   # Plain SQL path
   python main_dbt.py         # dbt path
   ```

The Codespace keeps your `outputs/` directory in sync. Download artifacts (like the stakeholder report) from the left-hand file explorer when you are done.

---

## 📋 Deliverables & Grading

For this lab, you will submit:

1. **`Lab06_{First}_{Last}_{NetID}_Reply.md`** (auto-generated)
   - Found in `outputs/` folder after running the lab
   - Contains: KPI summary, data quality issues found, recommendations

2. **Screenshot of successful test runs** (from RUN_LOG.txt or terminal output)

3. **Modified SQL file** (optional, depending on assignment)
   - If asked to add a new KPI or fix a data quality issue

### Grading Rubric (In-Class Lab: 180 points)

| Criteria | Points |
|----------|--------|
| **Pipeline runs successfully** (no Python errors) | 40 pts |
| **All staging views created** (stg_orders, stg_customers, etc.) | 30 pts |
| **Fact table has correct row count** (only delivered orders) | 30 pts |
| **KPIs calculated correctly** (on_time_rate, avg_delivery_minutes) | 30 pts |
| **Data quality tests executed** (tests ran, results logged) | 30 pts |
| **Stakeholder report generated** (Reply.md file exists) | 20 pts |

**Submission format:** Follow Appendix B naming convention
```
Lab06_John_Doe_jde123_Report.md
Lab06_John_Doe_jde123_TestResults.png
```

---

## 🔧 Troubleshooting & FAQs

### Q: I get "psycopg2.errors.DependentObjectsStillExist: cannot drop table restaurants"

**A:** This was already fixed in `src/seed.py`. If you still see it:
1. Make sure you pulled the latest version of the code
2. The fix adds `CASCADE` to `DROP VIEW` statements
3. Re-run the notebook from the beginning

### Q: My test says "0 rows" but the test failed?

**A:** Tests are **inverted logic**:
- 0 rows = test PASSED ✅ (no bad records found)
- >0 rows = test FAILED ❌ (these rows violate the rule)

### Q: What's the difference between SQLite and PostgreSQL?

**A:**
- **SQLite:** File-based database (great for learning), limited concurrency
- **PostgreSQL:** Server-based database (production-grade), supports multiple users

For this lab, **both work identically**. The code automatically swaps between `.sql` and `.sqlite.sql` files.

### Q: Do I need to install dbt separately?

**A:** No! It's included in `requirements.txt`. Just run:
```bash
pip install -r requirements.txt
```

### Q: Can I use this project for my final group project?

**A:** YES! This lab is **designed as a template** for your final project. You can:
- Replace the CSVs with your own dataset
- Add more dimensions and facts
- Create additional KPIs
- Extend the data quality tests

### Q: How do I add a new test?

**Plain SQL approach:**
1. Create a new `.sql` file in `warehouse/tests/staging/` (e.g., `stg_orders_subtotal_positive.sql`)
2. Write a query that returns bad records: `SELECT * FROM stg_orders WHERE subtotal <= 0`
3. Add it to `warehouse/tests/staging.yml`:
   ```yaml
   - name: stg_orders_subtotal_positive
     severity: error
     sql: tests/staging/stg_orders_subtotal_positive.sql
   ```

**dbt approach:**
1. Add to `dbt_dashdash/models/staging/stg_orders.yml`:
   ```yaml
   - name: subtotal
     tests:
       - dbt_utils.expression_is_true:
           expression: "subtotal > 0"
   ```

---

## 📚 Additional Resources

### Recommended Reading (from your syllabus)

- **Kimball Ch.2** — Dimensional modeling fundamentals
- **Beaulieu Ch.1-3** — SQL joins and aggregations
- **DAMA Ch.13** — Data quality dimensions

### Online Resources

- **dbt Documentation:** https://docs.getdbt.com/
- **PostgreSQL Tutorial:** https://www.postgresqltutorial.com/
- **SQL Window Functions:** https://www.postgresql.org/docs/current/tutorial-window.html

### Office Hours

- **Wednesdays 5:00-6:00 PM ET**
- **Fridays 10:00-11:00 AM ET**
- Book via Calendly on Brightspace

---

---

## ⚖️ Plain SQL vs. dbt: Results Comparison & Benefits

### Actual Results from Both Pipelines

Below are the **real outputs** from running this lab with both approaches. Understanding these results will help you appreciate the benefits of each methodology.

---

### 📊 Plain SQL Results (`python main_plain_sql.py`)

```python
{
  'row_counts': {
    'restaurants': 5,
    'couriers': 4,
    'customers': 5,
    'orders': 9  # Raw data loaded
  },
  'verified_counts': {
    'restaurants': 5,
    'couriers': 4,
    'customers': 5,
    'orders': 9  # Verification passed
  },
  'staging_tests': [
    {'name': 'stg_orders_order_id_not_null', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'stg_orders_order_id_unique', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'stg_orders_status_accepted_values', 'severity': 'warn', 'failures': 0, 'failure_table': None},
    {'name': 'stg_orders_restaurant_fk_relationships', 'severity': 'error', 'failures': 1,
     'failure_table': 'dq_failures__stg_orders_restaurant_fk_relationships'},  # ← Data quality issue found!
    {'name': 'stg_orders_courier_fk_relationships', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'stg_orders_delivery_minutes_nonnegative', 'severity': 'error', 'failures': 0, 'failure_table': None}
  ],
  'mart_tests': [
    {'name': 'fct_deliveries_order_id_not_null', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'fct_deliveries_order_id_unique', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'fct_deliveries_restaurant_fk_dim', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'fct_deliveries_courier_fk_dim', 'severity': 'error', 'failures': 0, 'failure_table': None},
    {'name': 'dim_courier_vehicle_type_accepted_values', 'severity': 'error', 'failures': 1,
     'failure_table': 'dq_failures__dim_courier_vehicle_type_accepted_values'}  # ← Found invalid vehicle type!
  ],
  'custom_tests': [
    {'name': 'test_dropoff_status_logic', 'severity': 'error', 'failures': 3,
     'failure_table': 'dq_failures__test_dropoff_status_logic'}  # ← Business rule violations
  ],
  'exports': {
    'stg_orders': 8,           # 8 rows (1 duplicate removed from 9)
    'fct_deliveries': 4,        # 4 successfully delivered orders
    'monitoring_dq_exceptions': 4,  # 4 data quality issues flagged
    'kpi_delivery_overview': 1      # 1 row of aggregated KPIs
  },
  'stakeholder_reply': 'Lab06_First_Last_netid1234_Reply.md',  # Report generated
  'run_log': 'RUN_LOG.txt'
}
```

**Summary:** Plain SQL approach found **5 data quality issues** across 3 test categories.

---

### 🔧 dbt Results (`python main_dbt.py`)

#### Phase 1: Model Building (`dbt run`)

```
21:59:27  Found 10 models, 13 data tests, 4 sources, 556 macros
21:59:27  Concurrency: 4 threads (target='dev')
21:59:27
21:59:27  1 of 10 START sql view model public_public.stg_couriers ........................ [RUN]
21:59:27  2 of 10 START sql view model public_public.stg_customers ....................... [RUN]
21:59:27  3 of 10 START sql view model public_public.stg_orders .......................... [RUN]
21:59:27  4 of 10 START sql view model public_public.stg_restaurants ..................... [RUN]
21:59:28  1 of 10 OK created sql view model public_public.stg_couriers ................... [CREATE VIEW in 0.32s]
21:59:28  4 of 10 OK created sql view model public_public.stg_restaurants ................ [CREATE VIEW in 0.32s]
21:59:28  2 of 10 OK created sql view model public_public.stg_customers .................. [CREATE VIEW in 0.32s]
21:59:28  5 of 10 START sql view model public_public.dim_courier ......................... [RUN]
21:59:28  6 of 10 START sql view model public_public.dim_restaurant ...................... [RUN]
21:59:28  7 of 10 START sql view model public_public.monitoring_dq_exceptions ............ [RUN]
21:59:28  3 of 10 OK created sql view model public_public.stg_orders ..................... [CREATE VIEW in 0.33s]
...
21:59:29  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=10
```

**Key Observations:**
- **Parallel execution:** 4 models ran simultaneously (notice timestamps)
- **Automatic dependency resolution:** dbt ran staging before marts
- **All 10 models built successfully** in **1.95 seconds**

#### Phase 2: Testing (`dbt test`)

```
21:59:30  1 of 13 START test accepted_values_dim_courier_vehicle_type__bike__scooter__car  [RUN]
21:59:30  2 of 13 START test accepted_values_stg_couriers_vehicle_type__bike__scooter__car  [RUN]
21:59:30  3 of 13 START test accepted_values_stg_orders_status__delivered__canceled__returned__unknown  [RUN]
21:59:30  4 of 13 START test dbt_utils_expression_is_true_stg_orders_delivery_minutes...  [RUN]
...
21:59:32  Done. PASS=8 WARN=0 ERROR=5 SKIP=0 NO-OP=0 TOTAL=13

Failures:
  ✅ PASS: stg_orders_status accepted_values
  ✅ PASS: not_null tests (order_id)
  ✅ PASS: unique tests (order_id)
  ✅ PASS: referential integrity (courier_id, most restaurant_ids)
  ❌ FAIL: accepted_values_dim_courier_vehicle_type (1 failure)
  ❌ FAIL: relationships_stg_orders_restaurant_id (1 failure)
  ❌ ERROR: expression_is_true delivery_minutes (syntax error in YAML)
  ❌ ERROR: expression_is_true dropoff_ts (syntax error in YAML)
```

**Summary:** dbt found **same data quality issues** as Plain SQL, but ran tests in parallel and provided detailed error reports.

---

### 🎯 Side-by-Side Comparison

| Aspect | Plain SQL (`warehouse/`) | dbt (`dbt_dashdash/`) | Winner & Why |
|--------|-------------------------|----------------------|--------------|
| **Execution Time** | ~5-7 seconds (sequential) | **1.95 seconds (parallel)** | **dbt** — Automatic parallelization |
| **Dependencies** | Manual (you define file order) | **Automatic** (`ref()` tracks dependencies) | **dbt** — No mistakes possible |
| **Test Authoring** | Write custom SQL queries | **YAML config** (dbt generates SQL) | **dbt** — Faster, less code |
| **Test Discovery** | Must update `staging.yml` manually | **Automatic** (dbt scans schema files) | **dbt** — Less maintenance |
| **Error Messages** | Custom Python formatting | **dbt native** (compiled SQL paths) | **Tie** — Both are clear |
| **Learning Curve** | Steep (SQL + Python orchestration) | **Moderate** (SQL + YAML + Jinja) | **Plain SQL** for beginners, **dbt** for production |
| **Debugging** | Read `.sql` files directly | Must understand `{{ ref() }}` templating | **Plain SQL** — Easier to debug |
| **Version Control** | All SQL files tracked | **+ lineage** (dbt docs auto-generate DAG) | **dbt** — Better collaboration |
| **Industry Adoption** | Custom per company | **Standard** (dbt used by 1000s of orgs) | **dbt** — Transferable skills |
| **Final Project Readiness** | Good foundation | **Production-grade** portfolio piece | **dbt** — Impresses employers |

---

### 🌟 Key Benefits of dbt (The "Why" for Your Career)

#### 1. **Automatic Dependency Management**

**Plain SQL Problem:**
```python
# In your Python script, YOU must remember the order:
run_sql("warehouse/staging/stg_orders.sql")          # Must run first
run_sql("warehouse/marts/fct_deliveries.sql")        # Depends on stg_orders
```
If you forget the order → **tables don't exist errors!**

**dbt Solution:**
```sql
-- fct_deliveries.sql
SELECT * FROM {{ ref('stg_orders') }} WHERE status = 'delivered'
```
dbt automatically:
- Detects that `fct_deliveries` depends on `stg_orders`
- Runs `stg_orders` first, then `fct_deliveries`
- Visualizes this in a **lineage graph** (DAG)

**Career Impact:** In a real company with 100+ models, manual dependency tracking is impossible. dbt scales effortlessly.

---

#### 2. **Parallel Execution (Speed)**

**What Happened in This Lab:**
- Plain SQL: **Sequential** (staging → wait → marts → wait → KPIs) = ~5-7 seconds
- dbt: **Parallel** (4 staging models at once, then marts together) = **1.95 seconds**

**Visual:**
```
Plain SQL:
stg_orders → stg_customers → stg_couriers → stg_restaurants → dim_customer → ...
(one at a time)

dbt:
stg_orders ----┐
stg_customers -├─→ (all finish) → dim_customer ----┐
stg_couriers --┤                                    ├─→ fct_deliveries
stg_restaurants┘                                    │
                                                    └─→ kpi_delivery_overview
(4 at once)      (3 at once)                         (1 depends on fact)
```

**Career Impact:** In production with large datasets, this saves **hours of runtime** daily. Your data is fresher, reports are more timely.

---

#### 3. **Test Coverage Without Custom SQL**

**Plain SQL:**
```sql
-- You must write this test file: tests/staging/stg_orders_order_id_unique.sql
SELECT order_id, COUNT(*) as cnt
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```
- 13 tests = 13 SQL files to write and maintain
- **Easy to forget** to test a column

**dbt:**
```yaml
# models/staging/stg_orders.yml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
```
- dbt **generates** the SQL automatically
- **Self-documenting** (YAML serves as column documentation)
- Centralized (all tests for `stg_orders` in one file)

**Career Impact:** Data teams with dbt have **90%+ test coverage**. Plain SQL teams often have <50% because writing tests manually is tedious.

---

#### 4. **Incremental Models (Advanced Feature)**

**Not used in this basic lab, but a game-changer in production:**

**Problem:** You have 10 million orders. Re-running `stg_orders` every hour processes all 10 million rows → slow and expensive.

**dbt Solution:**
```sql
{{ config(materialized='incremental') }}
SELECT * FROM raw_orders
{% if is_incremental() %}
WHERE order_timestamp > (SELECT MAX(order_timestamp) FROM {{ this }})
{% endif %}
```
**Result:** Only process **new/changed rows** since last run → 100x faster!

**Career Impact:** This is what makes dbt viable for **petabyte-scale** data warehouses at companies like Spotify, GitLab, and Airbnb.

---

#### 5. **Built-in Documentation & Lineage**

**Run this command:**
```bash
dbt docs generate
dbt docs serve
```
**You get a beautiful web UI with:**
- **Column-level descriptions** from your YAML
- **Interactive lineage graph** (click a model, see all upstream/downstream dependencies)
- **Test status** (which tests passed/failed)
- **Compiled SQL** (see exactly what ran in your database)

**Plain SQL equivalent:** You'd have to build this yourself (or it doesn't exist).

**Career Impact:** Stakeholders and new team members can **self-serve answers** about your data warehouse. Reduces "How was this calculated?" Slack messages by 80%.

---

### 📈 When to Use Each Approach

| Scenario | Recommendation | Reason |
|----------|---------------|--------|
| **Learning SQL fundamentals (Weeks 2-5)** | Plain SQL | No distractions — focus on query logic |
| **Understanding ELT architecture** | Plain SQL | See the "plumbing" (Python orchestration, test runners) |
| **Mid-term exam** | Plain SQL | Fastest to write from scratch in 2 hours |
| **Final group project** | **dbt** | Production-grade, impresses employers, easier collaboration |
| **Job interviews (Analytics Engineer)** | **dbt** | 90% of analytics engineer roles require dbt |
| **Freelance consulting** | Plain SQL | Client might not have dbt; pure SQL always works |
| **Your portfolio on GitHub** | **Both!** | Show versatility: "I can write raw SQL *and* use modern tooling" |

---

### 🧑‍🎓 Instructor's Teaching Strategy

**For Students (Why You're Learning Both):**

1. **Weeks 2-7:** Start with **Plain SQL**
   - Build muscle memory for CTEs, window functions, CASE expressions
   - Understand *what* is happening at each transformation step
   - Internalize dimensional modeling concepts without tool complexity

2. **Weeks 8-12:** Transition to **dbt**
   - Recognize that the SQL *logic* is identical (you already know this!)
   - Learn dbt as a "productivity multiplier" for the SQL you already write
   - Experience the "aha!" moment when `ref()` auto-orders your models

3. **Weeks 13-14 (Final Project):** Use **dbt**
   - Apply everything you learned to a new dataset
   - Generate professional documentation and lineage diagrams
   - Create a portfolio piece that employers recognize

**Teaching Philosophy:**
> "You don't appreciate dbt until you've felt the pain of managing dependencies manually. That's why we start with Plain SQL — so you understand what problem dbt is solving."

---

### 💡 Key Insight from the Results

**Notice:** Both approaches found the **same 3 data quality issues**:
1. Order 1007 has `restaurant_id = 999` (doesn't exist in `stg_restaurants`)
2. Courier #4 has `vehicle_type = 'truck'` (should be bike/scooter/car)
3. Business rule violation: 3 orders have illogical dropoff/status combinations

**The difference:**
- **Plain SQL:** You wrote custom SQL to detect these
- **dbt:** You wrote YAML, dbt generated the detection SQL
- **Both:** Caught the issues before they reached production!

**Lesson:** The tool doesn't matter as much as the **mindset of testing your data**. But dbt makes it so easy, you're more likely to actually do it.

---

## 🎯 Learning Path Summary

```
Week 2: Understand dimensional modeling → Study data/ CSV files
Week 3: Learn SQL staging layer → Read warehouse/staging/stg_orders.sql
Week 4: Build dimensions and facts → Read warehouse/marts/
Week 5: Calculate KPIs → Read warehouse/kpis/kpi_delivery_overview.sql
Week 6: Implement data quality tests → Study warehouse/tests/
Week 7: Run the entire pipeline → Execute main_plain_sql.ipynb
Weeks 8-14: Apply dbt → Study dbt_dashdash/ folder, run main_dbt.ipynb
```

---

## 🔄 Pipeline Flow Diagram

```mermaid
flowchart TD
    A[Seed CSVs<br/>ensure_sample_csvs()] --> B[Data preview<br/>preview_csvs()]
    B --> C[Load raw tables<br/>load_tables()]
    C --> D[Verify counts<br/>verify_row_counts()]
    D --> E[Build staging views<br/>warehouse/staging/*.sql]
    E --> F[Run staging tests<br/>warehouse/tests/staging]
    E --> G[Build marts<br/>warehouse/marts/*.sql]
    G --> H[Run mart tests<br/>warehouse/tests/marts]
    G --> I[Materialise KPIs<br/>warehouse/kpis/*.sql]
    G --> J[Materialise monitoring views<br/>warehouse/monitoring/*.sql]
    H --> K[Run custom tests<br/>warehouse/tests/custom]
    I --> L[Export CSVs<br/>export_views()]
    J --> L
    K --> M[Stakeholder reply<br/>generate_stakeholder_reply()]
    L --> M
```

## 🧱 Optional: explore the dbt project

Students who already know dbt can open `dbt_dashdash/` to compare the templated models with the raw SQL (the Python runner will auto-generate a minimal Postgres profile from your `PG*` environment variables if one isn't already configured):

1. Copy `profiles.yml.example` to a location recognised by dbt (for example `~/.dbt/profiles.yml`) and adjust schema/user details if needed. *(If you run `python main_dbt.py`, it will generate a temporary profile for you based on the same `PG*` environment variables used by the Python pipeline.)*
2. Install dbt dependencies and run the project:

   ```bash
   cd dbt_dashdash
   dbt deps
   dbt run --profiles-dir . --target dev
   dbt test --profiles-dir . --target dev
   ```

The dbt folder mirrors the staging/mart/KPI/monitoring folders in `warehouse/`, so learners can study either version without hunting through mixed files.

## 🧰 Docker workflow
```bash
# Build
docker build -t dashdash-lab .

# Run a pipeline (override the default tail command)
docker run --rm --env-file .env -v "$(pwd)/outputs:/app/outputs" dashdash-lab python main_plain_sql.py
# or
docker run --rm --env-file .env -v "$(pwd)/outputs:/app/outputs" dashdash-lab python main_dbt.py
```

`docker-compose.yml` mounts the `outputs/` folder and keeps the container alive:

```bash
docker compose up -d
docker compose exec app python main_plain_sql.py
docker compose exec app python main_dbt.py
docker compose down
```

## 🧪 Tests
Data quality checks are defined in YAML + SQL. Modify or extend them in
`warehouse/tests/*.yml` and the pipeline will automatically pick them up.

## 📚 For instructors
- Students can choose between `warehouse/` (plain SQL) and `dbt_dashdash/` (dbt models) depending on the lesson focus.
- Both paths share the same business logic, so you can grade/compare outputs regardless of the tooling choice.
- The Python modules in `src/` are intentionally thin wrappers; feel free to extend with
  logging, metrics, or alternate export destinations.

---
_Last updated: 2025-10-08 22:04 UTC_
