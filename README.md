# Data Wrangling Class Project

This is my submission for Udacity's Data Wrangling course as part of the Data Analyst nanodegree, which is part of the Western Governor University Bachelor of Science in Data Management and Data Analytics.

Using Python, I audited and cleaned an OpenStreetMap XML extract, reshaped it into a JSON file, loaded it into MongoDB, and further queried and audited in MondoDB using PyMongo.

See writeup.html for the report. See main.ipynb for the main script.

## Files:

- *clean_and_write.py*: Module to clean the XML (greater_bellingham.osm) and write it to bham.json
- *environment.yml*: Conda environment used. Definitely contains a lot of packages you don't need for this.
- *mongo_audit.py*: Module of PyMongo queries.
- *osm_structure_audit.py*: Module to investigate the XML document structure using pandas as a preliminary audit.
- *README.md*: This.
- *main.ipynb*: Verbosely annotated main script. Running from start to finish will repeat the full process of cleaning, writing, and loading. However, you will have to download the OSM extract yourself using the coordinates provided. Also, I discussed my auditing process with examples, but I didn't recreate it.
- *writeup.html*: Shortened report of the process. Abridged main.ipynb.

## References and resources:

- *OpenStreetMap*: https://www.openstreetmap.org/
- *Python's xml.etree.ElementTree module*: https://docs.python.org/3/library/xml.html
- *Pydata's pandas package*: https://pandas.pydata.org/
- *NumPy's numpy package*: https://numpy.org/
- *Python's re package*: https://docs.python.org/3/library/re.html
- *Python's functools.lru_cache module*: https://docs.python.org/3/library/functools.html
- *IPython's IPython.core.interactiveshell package*: https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.interactiveshell.html
- *PyPi's pymongo package*: https://pypi.org/project/pymongo/
- *Python's os package*: https://docs.python.org/3/library/os.html
- *Many StackOverflow posts*: https://stackoverflow.com/users/2391771/kaleb-coberly
