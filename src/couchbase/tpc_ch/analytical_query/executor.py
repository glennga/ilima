import logging
import abc

from src.couchbase.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractQueryRunnable(AbstractTPCCHRunnable, abc.ABC):
    def _keyspace_prefix(self):
        return f'{self.bucket_name}._default'

    def query_1(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        {self._keyspace_prefix()}.Orders O
            UNNEST      O.o_orderline OL
            WHERE       OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}"
            GROUP BY    OL.ol_number
            SELECT      OL.ol_number, SUM(OL.ol_quantity) AS sum_qty, SUM(OL.ol_amount) AS sum_amount,
                        AVG(OL.ol_quantity) AS avg_qty, AVG(OL.ol_amount) AS avg_amount, COUNT(*) AS count_order
            ORDER BY    OL.ol_number;
        """

    def query_6(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM    {self._keyspace_prefix()}.Orders O
            UNNEST  O.o_orderline OL
            WHERE   OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}" AND 
                    OL.ol_quantity BETWEEN 1 AND 100000
            SELECT  SUM(OL.ol_amount) AS revenue;
        """

    def query_12(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        {self._keyspace_prefix()}.Orders O
            UNNEST      O.o_orderline OL
            WHERE       O.o_entry_d <= OL.ol_delivery_d AND 
                        OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}"
            GROUP BY    O.o_ol_cnt
            SELECT      O.o_ol_cnt, 
                        SUM(CASE WHEN O.o_carrier_id = 1 OR O.o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count,
                        SUM(CASE WHEN O.o_carrier_id <> 1 OR O.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count
            ORDER BY    O.o_ol_cnt;
        """

    def query_14(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM    {self._keyspace_prefix()}.Orders O
            UNNEST  O.o_orderline OL
            JOIN    {self._keyspace_prefix()}.Item I
            ON      I.i_id = OL.ol_i_id
            WHERE   OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}"
            SELECT  100.00 * SUM(CASE WHEN I.i_data LIKE 'pr%' THEN OL.ol_amount ELSE 0 END) / 
                        (1 + SUM(OL.ol_amount)) AS promo_revenue;
        """

    def query_15(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            WITH        Revenue AS (
                            FROM        {self._keyspace_prefix()}.Orders O
                            UNNEST      O.o_orderline OL
                            JOIN        {self._keyspace_prefix()}.Stock S
                            ON          OL.ol_i_id = S.s_i_id AND OL.ol_supply_w_id = S.s_w_id
                            WHERE       OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}"
                            GROUP BY    ((S.s_w_id * S.s_i_id) % 10000)
                            SELECT      ((S.s_w_id * S.s_i_id) % 10000) AS supplier_no, 
                                        SUM(OL.ol_amount) AS total_revenue
                        )
            FROM        Revenue R
            JOIN        {self._keyspace_prefix()}.Supplier SU
            ON          SU.su_suppkey = R.supplier_no
            WHERE       R.total_revenue = ( 
                FROM        Revenue M
                SELECT      VALUE MAX (M.total_revenue)
            )[0]
            SELECT      SU.su_suppkey, SU.su_name, SU.su_address, SU.su_phone, R.total_revenue
            ORDER BY    SU.su_suppkey;
        """

    def query_20(self, date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        {self._keyspace_prefix()}.Supplier SU
            JOIN        {self._keyspace_prefix()}.Nation N
            ON          SU.su_nationkey = N.n_nationkey
            WHERE       SU.su_suppkey IN (
                            FROM        {self._keyspace_prefix()}.Orders O
                            UNNEST      O.o_orderline OL
                            JOIN        {self._keyspace_prefix()}.Stock S
                            ON          OL.ol_i_id = S.s_i_id
                            WHERE       S.s_i_id IN (
                                            FROM    {self._keyspace_prefix()}.Item I
                                            WHERE   I.i_data LIKE 'co%'
                                            SELECT  VALUE I.i_id
                                        ) AND 
                                        OL.ol_delivery_d BETWEEN "{date_1}" AND "{date_2}"
                            GROUP BY    S.s_i_id, S.s_w_id, S.s_quantity
                            HAVING      (100 * S.s_quantity) > SUM(OL.ol_quantity)
                            SELECT      VALUE ((S.s_w_id * S.s_i_id) % 10000)
                        ) AND 
                        N.n_name = 'Germany'
            SELECT      SU.su_name, SU.su_address
            ORDER BY    SU.su_name;
        """
