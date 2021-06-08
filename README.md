# ilima
This repository contains all scripts + queries used to evaluate the multi-valued indexing implementation on AsterixDB. There are two main experiment sets that this repository contains: 
1. **Internal benchmarking (measuring the "faff")**. This experiment set is meant to compare the existing atomic indexes in AsterixDB against our implementation of multi-valued indexes. To work around the obvious cardinality issue, the main comparison is between an _atomic value_ and a _collection with one element_. We measure the query performance, the ingestion performance, and the index maintenance performance. The dataset + queries used are from a homegrown inventory management example.
2. **Indexed vs. non-indexed benchmarking**. This experiment set is meant to compare queries that utilize AsterixDB's multi-valued indexes vs. queries that don't. The dataset + queries used are from a modified CH-benchmark (to more naturally represent `lineitems` in `orders`).

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

#### 