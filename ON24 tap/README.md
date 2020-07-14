Prerequisites:
```
python==3.8.3 #(either via miniconda, anaconda or source build)
```

Setup:
1. Install the tap as a package (make you're cd'd into the repo's directory):
```
pip install -e tap-on24
```

2. Install the target and singer-python packages:
```
pip install target-stitch
pip install singer-python
```

3. Create the config.json file and make sure it has the following fields:
```
{
    "client_id": integer, #provision a token from ON24 to fill these in
    "token_key": "string",
    "token_secret": "string",
    "datefiltermode": "updated", #edit this and filterorder to your preference or leave as-is
    "filterorder": "asc"
}
```

4. Create the stitch_config.json
```
{
  "client_id" : integer, #get these from your "Import API" integration in Stitch
  "token" : "string",
  "small_batch_url": "https://api.stitchdata.com/v2/import/batch", #edit this and the below to your preference or leave as-is
  "big_batch_url": "https://api.stitchdata.com/v2/import/batch",
  "batch_size_preferences": {}
}
```

5. Run tap in discovery mode to generate catalog.json:
```
tap-on24 --config config.json --discover > catalog.json
```

6. Edit catalog.json to your preferences 
-  necessary step, most of the tables are paginated so the state file records their page as it performs API calls
-  in discovery, the catalog assigns "currentpage" to all tables as the replication key but not all tables will have this as this is only included on paginated tables

7. Edit state.json to your preferences or leave as-is

8. Run the tap and pipe the output to the target then add a new entry to our state file:
```
tap-on24 --config config.json --catalog catalog.json --state state.json | target-stitch --config stitch_config.json > state.json
```

9. Done!

Note: If you wish to automate this process, you only need to put steps 8 and 9 on a loop either using cron or Airflow. 
If you want to be extra fancy about it, set this up on a server and let that manage the tap