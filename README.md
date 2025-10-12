# Río Negro Matchup

Python package and scripts to:  
- Find and download [Sentinel 2] satelite imagery to matchup with water quality field measurements;  
- Run [ACOLITE](https://hypercoast.org/) atmosferic Correction and water quality models;  
- Validate models derived from satelite imagery with field measurements;  

# use examples

## Organizing module
It will look for Water Quality data, clean and organize it.

In case of using OAN's field campaigns data:
```python
 python rionegromatchup/Organizing.py --mode campaigns
```
This process will read campaigns data, organize and clean its values, and then merge with stations data, writing the results to `./data/monitoring_data/campaigns_organized.csv`

Or using OAN's realtime monitoring data:
```python
python rionegromatchup/Organizing.py --mode realtime
```
As realtime monitoring data produces one file for each station, all files will be read and stacked into one DataFrame then merged with stations coordinates.
The results will be written to `./data/monitoring_data/Automatic_WQ_monitoring_stations.csv`
