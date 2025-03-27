# Data generate tool for tpch, tpcds

## Requirement

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### generate tpch-sf1 and load into hive

```shell
python src/gen_tpcds_data.py -o tpcds -s 1
bash ./tpcds/load_tpcds_data.sh ./tpcds
```

### generate tpch-sf1 and load into hive
```shell
python src/gen_tpch_data.py -o tpch -s 1
bash ./tpch/load_tpch_data.sh ./tpch
```

### generate ssb-sf1 and load into hive
