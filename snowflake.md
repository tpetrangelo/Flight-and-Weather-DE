# Snowflake Architecture

##### Storage
- Data not stored in snowflake, exists externally.
- Hybrid columnar storage (not rows)
    - Saved in blobs (parquet?)

##### Query Processing
- Where compute power goes (MPP) - Mass Parallel Processing
- Data warehouse
    - Sizes - # of Servers
        - XS - 1
        - S - 2
        - M - 4
        - L - 8
        - XL - 16
        - 4XL - 128
- Multi-Cluster
    - Addtional warehouses can be activated and added to a cluster

##### Cloud Services
- Managing infra
- Access control
- Security
- Optimizer
- Metadata


# Warehouses
``` SQL
ALTER WAREHOUSE FLIGHT_WEATHER_WH RESUME --starts up WH
ALTER WAREHOUSE FLIGHT_WEATHER_WH SET WAREHOUSE_SIZE = SMALL --changes WH size. SET changes properties/changes values
```
- Auto-Scaling can add WH clusters to grouping for better computing when computing is innefficient
    - Used with more users, more queries (like in afternoon)
- Not meant to be used for more complex queries (increase WH size for that)
##### Scaling Policies
- Standard
    - Favors starting addtl WH (performance)
- Economy
    - Favors conserving credits vs starting addtl WH (cost)

# Tables & Databases