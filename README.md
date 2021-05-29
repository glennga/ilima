# ilima
This repository contains all scripts + queries used to evaluate the multi-valued indexing implementation on AsterixDB. There are two main experiment sets that this repository contains: 
1. **Internal benchmarking (measuring the "faff")**. This experiment set is meant to compare the existing atomic indexes in AsterixDB against our implementation of multi-valued indexes. To work around the obvious cardinality issue, the main comparison is between an _atomic value_ and a _collection with one element_. We measure the query performance, the ingestion performance, and the index maintenance performance. The dataset + queries used are from a homegrown inventory management example.
2. **External benchmarking (measuring against other systems)**. This experiment set is meant to compare AsterixDB's multi-valued indexes against the multi-valued indexes of other systems. The systems compared are Couchbase and MongoDB. The dataset + queries used are from a modified CH-benchmark (to more naturally represent `lineitems` in `orders`).

## Repository Structure
This repository is structured as follows:
```
ilima-repo
|- config                      // Holds experiment / instance specific settings.
|- out                         // Output for experiment logs.
|- resources                   // Holds data / AsterixDB package.
|- src                         // All experiment-related scripts.
   |- asterixdb
      |- lower_bound           // Experiment suite to get mininum response time.
      |- shopalot              // Experiment suite for internal benchmarking. 
      |- tpc_ch                // Experiment suite for external benchmarking.
   |- couchbase
      |- lower_bound
      |- tpc_ch
   |- mongodb
      |- lower_bound
      |- tpc_ch
|- tools                       // Scripts specific to instance.
   |- setup                    // Scripts to setup the server / analysis nodes.
```

## Getting Started
1. A minimum of two nodes are required: one for the database server (with root access) and another for the database client. An additional third node is also supported, to feed results to an AsterixDB instance (for analysis only).

2. Ensure that the following dependencies are installed on both nodes: `python3`, `jq`. `docker` is additionally required for the database server node.

3. (Optional) To setup the analysis node, spin-up a working AsterixDB instance (separate from the one you want to perform experiments on). Run the `tools/setup/analysis.sqlpp` script on this node, and modify the `logging.json` file to point to this exact node. Note that everytime this server restarts, the feed must be reconnected with the command:
  ```
  USE BenchmarkAnalysis;
  START FEED ResultsSocketFeed;
  ```
  
4. Clone this repository onto both the database server and database client node.

5. Install the `requirements.txt` file for your Python environment on both the server and client nodes. To create a virtual environment (and subsequently launch this), run the commands below. For those more comfortable with `Docker`, a Dockerfile is also provided for your convenience.

   ```
   cd ilima
   
   # Create an environment inside the ilima/venv folder.
   python3 -m venv venv
   
   # Activate this environment.
   source venv/bin/activate
   
   # Install the requirements.
   python3 -m pip install -r requirements.txt
   ```

6. Launch the instance required for the experiment you want to launch.

7. Run the experiment you want! :-)

## Internal Benchmarking Experiments
### Overview

There are three dimensions to an internal benchmarking experiment: 1) the experiment program and 2) the dataset, and 3) the dataverse.

#### Experiment Programs

| Program Path                                      | Description                                                  |
| ------------------------------------------------- | ------------------------------------------------------------ |
| `src/asterixdb/shopalot/load_basic_dataset`       | Benchmarks the Algebricks-layer bulk-loading on a dataset w/o any secondary indexes. |
| `src/asterixdb/shopalot/load_indexed_dataset`     | Benchmarks the Algebricks-layer bulk-loading on a dataset with secondary indexes. |
| `src/asterixdb/shopalot/load_basic_dataverse`     | Executes the Hyracks-layer bulk-loading on a dataset w/o any secondary indexes. This is meant to be run before `equality_predicate_query` and `insert_upsert_delete`. |
| `src/asterixdb/shopalot/load_indexed_dataverse`   | Executes the Hyracks-layer bulk-loading on a dataset w/ secondary indexes. This is meant to be run before `equality_predicate_query` and `insert_upsert_delete`. |
| `src/asterixdb/shopalot/equality_predicate_query` | Benchmarks various equality / membership queries on some dataset. |
| `src/asterixdb/shopalot/insert_upsert_delete`     | Benchmarks the various maintenance operations (`INSERT`, `UPSERT`, `DELETE`) on some dataset. Note that this experiment is destructive, so `equality_predicate_query`  should be run before this. |

#### Dataset

Inside each experiment program directory, there are three programs: 1) `_orders.py`, 2) `_stores.py`, and 3) `_users.py`. Each correspond to the dataset below. 

| Dataset | Description                                                  |
| ------- | ------------------------------------------------------------ |
| Users   | Users of some inventory management service. Tests revolve around the low selectivity fields `phone` and `phones`. |
| Stores  | Stores associated with some inventory management service. Tests revolve around the high selectivity fields `category` and `categories`. |
| Orders  | Orders placed by users to stores in some inventory management service. Tests revolve around the high (but lower than Stores) selectivity fields `item.qty, item.product_id` and `qty in items, product_id in items`. |

#### Dataverse

As an argument to each experiment program, one must specify the dataverse: 1) `atom` or 2) `sarr`.

| Dataverse | Description                                                  |
| --------- | ------------------------------------------------------------ |
| `atom`    | Describes the atomic version of each dataset (i.e. no arrays). |
| `sarr`    | Describes the single-element array version of each dataset. This involves the multi-valued indexing code path. |

### Running Experiments

1. On the database server node, copy a packaged AsterixDB instance to the `resources` folder. Align this location with the `package` field in `config/asterixdb.json`.

2. Generate the ShopALot dataset on the database server node.
  1. Modify the `config/shopalot.json` to change the dataset sizes. The `idRange` field denotes how many records will be generated for that specific dataset (the difference between `end` and `start`), while the `chunkSize` field denotes how many records will be operated on at a time for a maintenance operation (`INSERT` | `UPSERT` | `DELETE`).
  2. Execute the datagen script. There are three datasets that this experiment works on: the `Users`, `Stores`, and `Orders`. This experiment is designed to run each dataset independently (to minimize the required disk space), so this process can be performed iteratively (i.e. generate dataset 1, run experiments, delete dataset 1, generate dataset 2, etc...) or all at once (i.e. generate all datasets, then run all experiments).
		```
    python3 src/asterixdbshopalot/datagen.py -h
    usage: datagen.py [-h] [--config CONFIG] {user,store,order}
    ```

3. Spin up an AsterixDB instance for the ShopALot experiment. This will spawn a Docker container running the specified AsterixDB instance named `asterixdb_`  with local networking.
   ```
   sudo ./tools/setup/container.sh asterixdb shopalot
   ```

4. Now switch to the database client node. Modify `config/asterixdb.json` and ensure that a) `benchmark.clusterController.address` points to your database server node and b) `benchmark.allNodesInCluster` only contains your database server node.

5. Run the experiments of your choosing! A good starting experiment that doesn't take too long (and acts as a sanity check) is the `lower_bound/_load.py` experiment.

   ```
   python3 src/asterixdb/lower_bound/_load.py
   ```
   
   Having read the section above on datasets and experiment programs, to run your desired experiment is to run `_orders.py`, `_stores.py`, or `_users.py` in the experiment program path of your choosing. As an example, to run the `load_basic_dataset` experiment on the Users dataset in the `atom` dataverse would be execute the following:
   
   ```
   python3 src/asterixdb/shopalot/load_basic_dataset/_users.py atom
   ```
   

## External Benchmarking Experiments

### Overview

There are three dimensions to an external benchmarking experiment: 1) the system being tested, 2) the system feature being tested, and 3) the experiment program.

#### Systems (and Features) Being Tested

| System Path                                                 | Description                                                  |
| ----------------------------------------------------------- | ------------------------------------------------------------ |
| `src/asterixdb/tpc_ch/{multivalued_indexing | no_indexing}` | AsterixDB (Our system :-)). There is one feature being tested here, which is our multi-valued indexing. We are also testing the performance gain / impact of not having an index. |
| `src/couchbase/tpc_ch`                                      | Couchbase. We are testing their array-indexing here (a covering index approach). |
| `src/mongodb/tpc_ch`                                        | MongoDB. We are testing their multi-key indexing. |

#### Experiment Program

There are three different experiment programs: 1) `_initialize.py`, 2) `_query.py`, and 3) `_maintain.py`.

| Program                           | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| Initialization (`_initialize.py`) | Loads the initial CH data. This should be run before any of the other programs. |
| Query (`_query.py`)               | Performs the CH queries that involve "unnesting" the `lineitems` array. These are analytical queries 1, 4, 6, 7, 8, 9, 12, 14, 15, 17, 19, 20, and 21. |
| Maintenance (`_maintain.py`)      | Performs the transactional portion of the CH benchmark, mainly to inform us of the index maintenance cost of each system. |

### Running Experiments

1. Generate the experiment data!
2. ...

#### AsterixDB

1. On the database server node, copy a packaged AsterixDB instance to the `resources` folder. Align this location with the `package` field in `config/asterixdb.json`.

2. Spin up an AsterixDB instance for the TPC-CH experiment. This will spawn a Docker container running the specified AsterixDB instance named `asterixdb_`  with local networking.

   ```
   sudo ./tools/setup/container.sh asterixdb tpc_ch
   ```

3. Now switch to the database client node. Modify `config/asterixdb.json` and ensure that a) `benchmark.clusterController.address` points to your database server node and b) `benchmark.allNodesInCluster` only contains your database server node.

#### Couchbase

#### MongoDB

#### MySQL