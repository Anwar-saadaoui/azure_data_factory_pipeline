# ADF Data Flow Transformation Chain + Azure SQL Loading — Working Notes

This is the consolidated, corrected reference from building and debugging the curated-zone Data Flow and loading it into Azure SQL. Everything here reflects what was actually tested and confirmed working, including the dead ends we hit along the way.

---

## 1. Confirmed source schema (Source → Projection tab)

| # | Column | Type |
|---|---|---|
| 1 | VendorID | integer |
| 2 | tpep_pickup_datetime | long |
| 3 | tpep_dropoff_datetime | long |
| 4 | passenger_count | long (was showing as `any` until manually set in Projection tab) |
| 5 | trip_distance | double |
| 6 | RatecodeID | long (was showing as `any` until manually set in Projection tab) |
| 7 | store_and_fwd_flag | string |
| 8 | PULocationID | integer |
| 9 | DOLocationID | integer |
| 10 | payment_type | long |
| 11 | fare_amount | double |
| 12 | extra | double |
| 13 | mta_tax | double |
| 14 | tip_amount | double |
| 15 | tolls_amount | double |
| 16 | improvement_surcharge | double |
| 17 | total_amount | double |
| 18 | congestion_surcharge | double |
| 19 | Airport_fee | double |
| 20 | cbd_congestion_fee | double |

**Key lesson:** even when a column's schema is confirmed at the file level, ADF's Source projection can independently resolve it as `any` (unresolved type). This breaks any downstream expression referencing that column, with error messages that vary depending on what you're trying to do with it. **Fix:** go into Source → Projection tab and manually set the type dropdown for any column showing `any`, rather than patching around it in Derived Column expressions.

---

## 2. Data Flow transformation chain (built and working)

```
Source → WindowDedup → FilterDedup → SelectDropRowNum → HandleNulls
       → FixTypes → CalcRevenue → FilterInconsistent → Sink
```

### Step: WindowDedup (Window transformation)
- **Over**: `VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID`
- **Sort**: `tpep_pickup_datetime` ascending
- **Window columns**: new column `row_num` = `rowNumber()`

### Step: FilterDedup (Filter)
```
row_num == 1
```

### Step: SelectDropRowNum (Select)
Drop `row_num` before it flows further.

### Step: HandleNulls (Derived Column)
```
passenger_count = iif(isNull(passenger_count), 1L, passenger_count)
RatecodeID = iif(isNull(RatecodeID), 1L, RatecodeID)
```
Note the `L` suffix — matches the `long` type. A bare `1` mismatches against `long`; a bare `1.0` mismatches if the column resolves as `long` rather than `double`. **Always match the literal suffix to the column's actual resolved type in the Projection tab, not the type you assume it should be.**

### Step: FixTypes (Derived Column)
```
tpep_pickup_datetime  = toTimestamp(tpep_pickup_datetime)
tpep_dropoff_datetime = toTimestamp(tpep_dropoff_datetime)

fare_amount            = toDecimal(fare_amount, 10, 2)
extra                  = toDecimal(extra, 10, 2)
mta_tax                = toDecimal(mta_tax, 10, 2)
tip_amount             = toDecimal(tip_amount, 10, 2)
tolls_amount           = toDecimal(tolls_amount, 10, 2)
improvement_surcharge  = toDecimal(improvement_surcharge, 10, 2)
total_amount           = toDecimal(total_amount, 10, 2)
congestion_surcharge   = toDecimal(congestion_surcharge, 10, 2)
Airport_fee            = toDecimal(Airport_fee, 10, 2)
cbd_congestion_fee     = toDecimal(cbd_congestion_fee, 10, 2)
```
`PULocationID`/`DOLocationID` already `integer` — no cast needed, left untouched.

### Step: CalcRevenue (Derived Column)
```
revenue = fare_amount + tip_amount + tolls_amount + extra + mta_tax + improvement_surcharge + congestion_surcharge + Airport_fee + cbd_congestion_fee
```

### Step: FilterInconsistent (Filter)
```
tpep_dropoff_datetime > tpep_pickup_datetime
&& trip_distance > 0 && trip_distance < 100
&& fare_amount > 0
```
(`trip_duration_min` condition intentionally left out — see unresolved issue below.)

### Step: Sink
- Curated zone: `curated/yellow_tripdata/`
- Format: Parquet
- File name pattern: `part-[n].parquet`
- **Actual result:** 5 partition files written (`part-1.parquet` ... `part-5.parquet`, ~15MB each) — confirmed working in ADLS Gen2 container browser.

---

## 3. Unresolved issue: `trip_duration_min`

**Goal:** `trip_duration_min = (dropoff - pickup) / 60` in minutes.

**Problem:** ADF Data Flows have no dedicated "minutes between two timestamps" function, and there is no supported function to convert a `timestamp` back to a raw `long`/epoch value (`toLong()` does not accept `timestamp` input). Multiple approaches were attempted and failed:

| Attempt | Result |
|---|---|
| `toTimestamp(tpep_pickup_datetime / 1000L)` | Type mismatch — `/` always returns `double`, `toTimestamp` needs long |
| `toTimestamp(div(tpep_pickup_datetime, 1000L))` | Still mismatched |
| `addSeconds(toTimestamp('1970-01-01...'), toLong(tpep_pickup_datetime/1000L))` | Overcomplicated, still mismatched |
| `toTimestamp(tpep_pickup_datetime)` directly (docs confirm this accepts raw milliseconds) | This part actually works for the timestamp cast itself |
| `div(tpep_dropoff_datetime - tpep_pickup_datetime, 60000L)` computed **after** timestamp cast | Mismatch — subtracting two `timestamp` values resolves inconsistently (sometimes `any`, not a clean millisecond number) |
| `toLong(tpep_dropoff_datetime) - toLong(tpep_pickup_datetime)` | Mismatch — `toLong()` does not accept `timestamp` input at all |

**Two known-valid ways forward (not yet applied in the live pipeline):**

**Option A — preserve raw longs in separate columns before FixTypes overwrites them:**
```
# Add before FixTypes:
pickup_epoch_ms = tpep_pickup_datetime
dropoff_epoch_ms = tpep_dropoff_datetime

# Then after FixTypes, in CalcRevenue step or its own step:
trip_duration_min = div(dropoff_epoch_ms - pickup_epoch_ms, 60000L)
```

**Option B — don't overwrite originals; create new timestamp columns instead:**
```
# In FixTypes, instead of overwriting:
tpep_pickup_ts  = toTimestamp(tpep_pickup_datetime)
tpep_dropoff_ts = toTimestamp(tpep_dropoff_datetime)
# tpep_pickup_datetime / tpep_dropoff_datetime remain raw longs, untouched

# Duration calc anywhere after:
trip_duration_min = div(tpep_dropoff_datetime - tpep_pickup_datetime, 60000L)
```

**Recommended actual fix — compute it in SQL instead, after loading:**
```sql
ALTER TABLE fact_trips ADD trip_duration_min AS DATEDIFF(MINUTE, pickup_datetime, dropoff_datetime);
```
This is a **computed column** — SQL Server calculates it automatically and correctly from the two real `datetime2` columns already in the table, sidestepping the entire ADF expression-language limitation. This is the path currently being taken for this project.

---

## 4. Azure SQL Database — connection & auth troubleshooting

### Problem: server was created with Entra ID-only authentication
No SQL login/password existed — only Entra ID auth was possible initially.

### Attempted: grant ADF's Managed Identity access directly
```sql
CREATE USER [adf-nyc-taxi] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [adf-nyc-taxi];
ALTER ROLE db_datawriter ADD MEMBER [adf-nyc-taxi];
```
**Result:** `Principal 'adf-nyc-taxi' could not be found or this principal type is not supported.`
Likely cause: Data Factory's System Assigned Managed Identity wasn't enabled/propagated, and/or name mismatch. Not pursued further — switched to SQL auth instead for simplicity.

### Fix applied: enable SQL Authentication alongside Entra ID
1. Portal → SQL **server** resource → **Settings → Authentication**.
2. Uncheck **"Support only Microsoft Entra authentication for this server."**
3. Save. (This is an ARM/resource-level setting — cannot be done via T-SQL.)

### Create the SQL login (must run against `master`, not the user database)
Error hit: `User must be in the master database.`
**Fix:** open a fresh connection with database explicitly set to `master`, then run:
```sql
CREATE LOGIN sqladmin WITH PASSWORD = 'YourNewStrongPassword123!';
```
Then, back in the target database (`nyctaxidb`):
```sql
CREATE USER sqladmin FOR LOGIN sqladmin;
ALTER ROLE db_datareader ADD MEMBER sqladmin;
ALTER ROLE db_datawriter ADD MEMBER sqladmin;
```

### ADF Linked Service (Sink) — SQL Authentication
- Server: `sql-nyctaxi.database.windows.net`
- Database: `nyctaxidb`
- Authentication type: **SQL Authentication**
- Username/password: as created above
- Test connection → Create.

---

## 5. Loading curated data into Azure SQL

### Star schema tables (created once, via SSMS/Azure Data Studio)
```sql
CREATE TABLE stg_curated_trips (
    VendorID INT,
    tpep_pickup_datetime DATETIME2,
    tpep_dropoff_datetime DATETIME2,
    passenger_count INT,
    trip_distance DECIMAL(10,2),
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    revenue DECIMAL(10,2)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day INT, month INT, month_name VARCHAR(20),
    quarter INT, year INT,
    day_of_week VARCHAR(10), is_weekend BIT
);

CREATE TABLE dim_zone (
    zone_key INT IDENTITY(1,1) PRIMARY KEY,
    location_id INT,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

CREATE TABLE dim_payment_type (
    payment_type_key INT PRIMARY KEY,
    payment_type_id INT,
    payment_type_desc VARCHAR(30)
);

CREATE TABLE fact_trips (
    trip_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    pickup_zone_key INT FOREIGN KEY REFERENCES dim_zone(zone_key),
    dropoff_zone_key INT FOREIGN KEY REFERENCES dim_zone(zone_key),
    payment_type_key INT FOREIGN KEY REFERENCES dim_payment_type(payment_type_key),
    pickup_datetime DATETIME2,
    dropoff_datetime DATETIME2,
    passenger_count INT,
    trip_distance DECIMAL(10,2),
    fare_amount DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    revenue DECIMAL(10,2)
);
```

### ADF Copy Data activity — curated parquet → stg_curated_trips
- **Source dataset**: ADLS Gen2, Parquet
  - File path type: **Wildcard file path**
  - Wildcard folder path: `curated/yellow_tripdata` *(confirmed real path — no year/month subfolders in this project's actual layout, despite earlier plan)*
  - Wildcard file name: `*.parquet`
- **Sink dataset**: Azure SQL Database, table `stg_curated_trips`
- **Mapping tab**: Import schemas / auto-map by matching column names
- Run via **Debug**

### Error hit: `PathNotFound` — `curated/yellow_tripdata` not found
Root cause: dataset was pointed at `curated/yellow_tripdata/2025/01` per the original plan, but the actual folder created during ingestion was just `curated/yellow_tripdata/`. **Always verify the real path in the Portal container browser rather than assuming it matches an earlier plan.**

### Populate dim_date (one-time script)
```sql
DECLARE @StartDate DATE = '2025-01-01';
DECLARE @EndDate DATE = '2025-01-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dim_date (date_key, full_date, day, month, month_name, quarter, year, day_of_week, is_weekend)
    VALUES (
        CONVERT(INT, FORMAT(@StartDate, 'yyyyMMdd')),
        @StartDate,
        DAY(@StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DATEPART(QUARTER, @StartDate),
        YEAR(@StartDate),
        DATENAME(WEEKDAY, @StartDate),
        CASE WHEN DATENAME(WEEKDAY, @StartDate) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
    );
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END
```

### Populate dim_payment_type (one-time script)
```sql
INSERT INTO dim_payment_type (payment_type_key, payment_type_id, payment_type_desc) VALUES
(1, 1, 'Credit card'),
(2, 2, 'Cash'),
(3, 3, 'No charge'),
(4, 4, 'Dispute'),
(5, 5, 'Unknown'),
(6, 6, 'Voided trip');
```

### Populate dim_zone
Separate ADF Copy Data activity: `taxi_zone_lookup.csv` → `dim_zone` table. Map `LocationID → location_id`, `Borough → borough`, `Zone → zone`, `service_zone → service_zone`.

### Populate fact_trips (final SQL step, joins staging to dims)
```sql
INSERT INTO fact_trips
    (date_key, pickup_zone_key, dropoff_zone_key, payment_type_key,
     pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
     fare_amount, tip_amount, tolls_amount, revenue)
SELECT
    CONVERT(INT, FORMAT(c.tpep_pickup_datetime, 'yyyyMMdd')),
    puz.zone_key, doz.zone_key, pt.payment_type_key,
    c.tpep_pickup_datetime, c.tpep_dropoff_datetime, c.passenger_count,
    c.trip_distance, c.fare_amount, c.tip_amount, c.tolls_amount, c.revenue
FROM stg_curated_trips c
JOIN dim_zone puz ON puz.location_id = c.PULocationID
JOIN dim_zone doz ON doz.location_id = c.DOLocationID
JOIN dim_payment_type pt ON pt.payment_type_id = c.payment_type;
```

### Add trip_duration_min as a computed column (resolves the ADF issue permanently)
```sql
ALTER TABLE fact_trips ADD trip_duration_min AS DATEDIFF(MINUTE, pickup_datetime, dropoff_datetime);
```

### Verify relationship integrity
```sql
SELECT COUNT(*) FROM fact_trips f
LEFT JOIN dim_zone z ON f.pickup_zone_key = z.zone_key
WHERE z.zone_key IS NULL;   -- should be 0

SELECT COUNT(*) FROM fact_trips;
```

---

## 6. Status as of last check

- ✅ Curated zone parquet confirmed written (5 part files, ~62MB total).
- ✅ SQL auth enabled and working (`sqladmin` login created against `master`, granted access on `nyctaxidb`).
- ✅ All 5 tables created (`stg_curated_trips`, `dim_date`, `dim_zone`, `dim_payment_type`, `fact_trips`).
- 🔄 **In progress**: Copy Data activity `curated/yellow_tripdata` → `stg_curated_trips`, currently running past 5 minutes on first Debug attempt after fixing the wildcard path. Longer-than-expected duration under investigation — likely first-run Integration Runtime cold start; give it up to ~10 minutes before treating as stuck, then cancel and retry if `rowsCopied` shows no movement at all.
- ⏳ Not yet done: dim_date/dim_payment_type population, dim_zone Copy Data activity, fact_trips join-load, trip_duration_min computed column.
