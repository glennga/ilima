import logging
import abc

from src.asterixdb.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractLoadDataverseRunnable(AbstractTPCCHRunnable, abc.ABC):
    def _create_dataverse(self):
        data_path = 'localhost:///' + self.config['tpc_ch']['dataPath']
        results = self.execute_sqlpp(f"""
            DROP DATAVERSE TPC_CH IF EXISTS;
            CREATE DATAVERSE TPC_CH;
            USE TPC_CH;
            
            CREATE TYPE CustomerType AS {{ c_w_id: bigint, c_d_id: bigint, c_id: bigint }};
            CREATE DATASET Customer (CustomerType) PRIMARY KEY c_w_id, c_d_id, c_id;
            LOAD DATASET TPC_CH.Customer USING localfs ( ("path"="{data_path}/customer.json"), ("format"="json") );

            CREATE TYPE NationType AS {{ n_nationkey: bigint }};
            CREATE DATASET Nation (NationType) PRIMARY KEY n_nationkey;
            LOAD DATASET TPC_CH.Nation USING localfs ( ("path"="{data_path}/nation.json"), ("format"="json") );

            CREATE TYPE OrdersType AS {{ o_w_id: bigint, o_d_id: bigint, o_id: bigint  }};
            CREATE DATASET Orders (OrdersType) PRIMARY KEY o_w_id, o_d_id, o_id;
            LOAD DATASET TPC_CH.Orders USING localfs ( ("path"="{data_path}/orders.json"), ("format"="json") );

            CREATE TYPE StockType AS {{ s_w_id: bigint, s_i_id: bigint }};
            CREATE DATASET Stock (StockType) PRIMARY KEY s_w_id, s_i_id;
            LOAD DATASET TPC_CH.Stock USING localfs ( ("path"="{data_path}/stock.json"), ("format"="json") );    

            CREATE TYPE ItemType AS {{ i_id: bigint }};
            CREATE DATASET Item (ItemType) PRIMARY KEY i_id;
            LOAD DATASET TPC_CH.Item USING localfs ( ("path"="{data_path}/item.json"), ("format"="json") );            

            CREATE TYPE RegionType AS {{ r_regionkey: bigint }};
            CREATE DATASET Region (RegionType) PRIMARY KEY r_regionkey;
            LOAD DATASET TPC_CH.Region USING localfs ( ("path"="{data_path}/region.json"), ("format"="json") );            

            CREATE TYPE SupplierType AS {{ su_suppkey: bigint }};
            CREATE DATASET Supplier (SupplierType) PRIMARY KEY su_suppkey;            
            LOAD DATASET TPC_CH.Supplier USING localfs ( ("path"="{data_path}/supplier.json"), ("format"="json") );            
        """)
        self.log_results(results)
