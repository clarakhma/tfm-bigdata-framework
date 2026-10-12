# Data

This folder holds the public reference data the pipeline needs. Large raw datasets and private data are **not** stored here; they are referenced below.

## Included (public exogenous data)

| File | Used by | Description |
|---|---|---|
| `madrid_festivos.csv` | Energy ETL (`add_holiday_flag`) | Public holiday calendar for Madrid, Spain. |
| `madrid_meteo_api.csv` | Energy ETL (`join_reference`) | Daily temperature for Madrid (Open-Meteo API export). |
| `milano_grid_centroids.csv` | Telecom ETL (`join_reference`) | Centroid (lat/lon) of each of the 10,000 Milan grid cells, used to add geographic coordinates and the distance to the city centre. Derived from the official Milan grid (`milano-grid.geojson`). |

> Note: if `milano_grid_centroids.csv` is missing from this folder, copy it from the project configuration bucket (`s3://tfm-config-ckm-ew1/exog/`) or regenerate it from `milano-grid.geojson` (one centroid per polygon).

## Not included: referenced only

### Telecom raw data (public, large)

The telecom raw files (`sms-call-internet-mi-YYYY-MM-DD.txt`) are **not** stored in this repository because of their size. They are public and come from:

> Barlacchi, G., De Nadai, M., Larcher, R. et al. *A multi-source dataset of urban life in the city of Milan and the Province of Trentino.* Scientific Data 2, 150055 (2015). DOI: [10.1038/sdata.2015.55](https://doi.org/10.1038/sdata.2015.55). Licensed under CC BY 4.0.

The specific files used are the Milan telecommunications activity grid (SMS, calls, internet per cell and 10-minute interval). The download location is linked from the paper's *Data Records* section (Harvard Dataverse).

### Energy raw data (private)

The energy consumption data (`data-energy.csv`) is **not** included. It belongs to a Spanish electricity company and is covered by a non-disclosure agreement, so it cannot be published.
