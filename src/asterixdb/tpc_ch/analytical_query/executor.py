import logging
import abc

from src.asterixdb.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractQueryRunnable(AbstractTPCCHRunnable, abc.ABC):
    @staticmethod
    def query_1(date_1):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        Orders O, O.o_orderline OL
            WHERE       OL.ol_delivery_d > '{date_1}'
            GROUP BY    OL.ol_number
            SELECT      OL.ol_number, SUM(OL.ol_quantity) AS sum_qty, SUM(OL.ol_amount) AS sum_amount,
                        AVG(OL.ol_quantity) AS avg_qty, AVG(OL.ol_amount) AS avg_amount, COUNT(*) AS count_order
            ORDER BY    OL.ol_number;
        """

    @staticmethod
    def query_6(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM    Orders O, O.o_orderline OL
            WHERE   OL.ol_delivery_d >= '{date_1}' AND 
                    OL.ol_delivery_d < '{date_2}' AND 
                    OL.ol_quantity BETWEEN 1 AND 100000
            SELECT  SUM(OL.ol_amount) AS revenue;
        """

    @staticmethod
    def query_7(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        Supplier SU, Stock S, Orders O, O.o_orderline OL, Customer C, Nation N1, Nation N2
            WHERE       OL.ol_supply_w_id = S.s_w_id AND
                        OL.ol_i_id = S.s_i_id AND
                        ((S.s_w_id * S.s_i_id) % 10000) = SU.su_suppkey AND
                        C.c_id = O.o_c_id AND
                        C.c_w_id = O.o_w_id AND
                        C.c_d_id = O.o_d_id AND
                        SU.su_nationkey = N1.n_nationkey AND
                        STRING_TO_CODEPOINT(SUBSTR(C.c_state, 1, 1))[0]  = N2.n_nationkey AND
                        ( ( N1.n_name = 'Germany' AND N2.n_name = 'Cambodia' ) OR
                          ( N1.n_name = 'Cambodia' AND N2.n_name = 'Germany' ) ) AND
                        OL.ol_delivery_d BETWEEN '{date_1}' AND '{date_2}'
            GROUP BY    SU.su_nationkey, STRING_TO_CODEPOINT(SUBSTR(C.c_state, 1, 1))[0], SUBSTR(O.o_entry_d, 0, 4)
            SELECT      SU.su_nationkey AS supp_nation, STRING_TO_CODEPOINT(SUBSTR(C.c_state, 1, 1))[0] AS cust_nation,
                        SUBSTR(O.o_entry_d, 0, 4) AS l_year, SUM(OL.ol_amount) AS revenue
            ORDER BY    SU.su_nationkey, cust_nation, l_year;
        """

    @staticmethod
    def query_8(date_1, date_2, hint='/*+ indexnl */'):
        """ Can utilize an index on the ol_i_id field for INLJ. """
        return f"""
            FROM        Item I, Supplier SU, Stock S, Orders O, O.o_orderline OL, Customer C, Nation N1, 
                        Nation N2, Region R 
            WHERE       I.i_id = S.s_i_id AND 
                        OL.ol_i_id {hint} = S.s_i_id AND 
                        OL.ol_supply_w_id = S.s_w_id AND 
                        ((S.s_w_id * S.s_i_id) % 10000) = SU.su_suppkey AND 
                        C.c_id = O.o_c_id AND 
                        C.c_w_id = O.o_w_id AND 
                        C.c_d_id = O.o_d_id AND 
                        N1.n_nationkey = STRING_TO_CODEPOINT(SUBSTR(C.c_state, 1, 1))[0] AND 
                        N1.n_regionkey = R.r_regionkey AND 
                        OL.ol_i_id < 1000 AND 
                        R.r_name = 'Europe' AND 
                        SU.su_nationkey = N2.n_nationkey AND 
                        O.o_entry_d BETWEEN '{date_1}' AND '{date_2}' AND 
                        I.i_data LIKE '%b' AND 
                        OL.ol_i_id {hint} = I.i_id
            GROUP BY    SUBSTR(O.o_entry_d, 0, 4)
            SELECT      SUBSTR(O.o_entry_d, 0, 4) AS l_year, 
                        SUM(CASE WHEN N2.n_name = 'Germany' THEN OL.ol_amount ELSE 0 END) / SUM(OL.ol_amount)
            ORDER BY    l_year;
        """

    @staticmethod
    def query_12(date_1):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        Orders O, O.o_orderline OL
            WHERE       OL.ol_delivery_d < '{date_1}' AND 
                        O.o_entry_d <= OL.ol_delivery_d
            GROUP BY    O.o_ol_cnt
            SELECT      O.o_ol_cnt, 
                        SUM(CASE WHEN O.o_carrier_id = 1 OR O.o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count,
                        SUM(CASE WHEN O.o_carrier_id <> 1 OR O.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count
            ORDER BY    O.o_ol_cnt;
        """

    @staticmethod
    def query_14(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM    Item I, Orders O, O.o_orderline OL
            WHERE   OL.ol_i_id = I.i_id AND 
                    OL.ol_delivery_d >= '{date_1}' AND
                    OL.ol_delivery_d < '{date_2}'
            SELECT  100.00 * SUM(CASE WHEN I.i_data LIKE 'pr%' THEN OL.ol_amount ELSE 0 END) / 
                        (1 + SUM(OL.ol_amount)) AS promo_revenue
        """

    @staticmethod
    def query_15(date_1):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            WITH        Revenue AS (
                            FROM        Stock S, Orders O, O.o_orderline OL
                            WHERE       OL.ol_i_id = S.s_i_id AND 
                                        OL.ol_supply_w_id = S.s_w_id AND
                                        OL.ol_delivery_d >= '{date_1}'
                            GROUP BY    ((S.s_w_id * S.s_i_id) % 10000)
                            SELECT      ((S.s_w_id * S.s_i_id) % 10000) AS supplier_no, 
                                        SUM(OL.ol_amount) AS total_revenue
                        )
            FROM        Supplier SU, Revenue R
            WHERE       SU.su_suppkey = R.supplier_no AND 
                        R.total_revenue = ( SELECT VALUE MAX(total_revenue) FROM Revenue )[0]
            SELECT      SU.su_suppkey, SU.su_name, SU.su_address, SU.su_phone, R.total_revenue
            ORDER BY    SU.su_suppkey;
        """

    @staticmethod
    def query_20(date_1):
        """ Can utilize an index on the ol_delivery_d field. """
        return f"""
            FROM        Supplier SU, Nation N
            WHERE       SU.su_suppkey IN (
                            FROM        Stock S, Orders O, O.o_orderline OL
                            WHERE       S.s_i_id IN (
                                            SELECT  VALUE I.i_id
                                            FROM    Item I
                                            WHERE   I.i_data LIKE 'co%'
                                        ) AND 
                                        OL.ol_i_id = S.s_i_id AND 
                                        OL.ol_delivery_d > '{date_1}'
                            GROUP BY    S.s_i_id, S.s_w_id, S.s_quantity
                            HAVING      (100 * S.s_quantity) > SUM(OL.ol_quantity)
                            SELECT      VALUE ((S.s_w_id * S.s_i_id) % 10000)
                        ) AND 
                        SU.su_nationkey = N.n_nationkey AND 
                        N.n_name = 'Germany'
            SELECT      SU.su_name, SU.su_address
            ORDER BY    SU.su_name;
        """
