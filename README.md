# Río Negro Matchup

Python package and scripts to:  
- Find and download [Sentinel 2] satelite imagery to matchup with water quality field measurements;  
- Run [ACOLITE](https://hypercoast.org/) atmosferic Correction and water quality models;  
- Validate models derived from satelite imagery with field measurements;  



## Exemplos de uso

### Só gerar o catálogo JSON (sem download):

```python
python sentinel_pipeline.py --mode catalog --csv datos/mediciones/mediciones_campo.csv --geojson datos/bbox_rincon.geojson --output datos/sentinel_downloads --time-delta 2
```


### Só baixar a partir do JSON existente:

```python
python sentinel_pipeline.py --mode download --csv datos/mediciones/mediciones_campo.csv --geojson datos/bbox_rincon.geojson --output datos/sentinel_downloads --catalog-json datos/sentinel_catalog.json
```

### Rodar tudo de uma vez (catalog + download):

```python
python sentinel_pipeline.py --mode all --csv datos/mediciones/mediciones_campo.csv --geojson datos/bbox_rincon.geojson --output datos/sentinel_downloads --only-first
```

