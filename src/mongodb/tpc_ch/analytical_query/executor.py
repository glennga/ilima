import logging
import abc

from src.mongodb.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractQueryRunnable(AbstractTPCCHRunnable, abc.ABC):
    @staticmethod
    def query_1(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return {
            'name': 'Orders',
            'predicate': None,
            'aggregate': [
                {
                    '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                },
                {
                    '$unwind': {'path': '$o_orderline'}
                },
                {
                    '$group': {'_id': '$o_orderline.ol_number',
                               'sum_qty': {'$sum': '$o_orderline.ol_quantity'},
                               'sum_amount': {'$sum': '$o_orderline.ol_amount'},
                               'avg_qty': {'$avg': '$o_orderline.ol_quantity'},
                               'avg_amount': {'$avg': '$o_orderline.ol_amount'},
                               'count_order': {'$sum': 1}}
                },
                {
                    '$sort': {'o_orderline.ol_number': 1}
                }
            ]
        }

    @staticmethod
    def query_6(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return {
            'name': 'Orders',
            'predicate': None,
            'aggregate': [
                {
                    '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                },
                {
                    '$match': {'o_orderline.ol_quantity': {'$gte': 1, '$lte': 100000}}
                },
                {
                    '$group': {'_id': None,
                               'revenue': {'$sum': '$o_orderline.ol_amount'}, }
                }
            ]
        }

    @staticmethod
    def query_12(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return {
            'name': 'Orders',
            'predicate': None,
            'aggregate': [
                {
                    '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                },
                {
                    '$match': {'o_entry_d': {'lte': '$o_orderline.ol_delivery_d'}}
                },
                {
                    '$project': {
                        'o_ol_cnt': '$o_ol_cnt',
                        'high_line': {
                            '$switch': {'branches': [{'case': {'$in': ['$o_carrier_id', [1, 2]]}, 'then': 1}],
                                        'default': 0}
                        },
                        'low_line': {
                            '$switch': {'branches': [{'case': {'$in': ['$o_carrier_id', [1, 2]]}, 'then': 0}],
                                        'default': 1}
                        }
                    }
                },
                {
                    '$group': {'_id': '$o_ol_cnt',
                               'high_line_count': {'$sum': 'high_line'},
                               'low_line_count': {'$sum': 'low_line'}}
                },
                {
                    '$sort': {'o_ol_cnt': 1}
                }
            ]
        }

    @staticmethod
    def query_14(date_1, date_2):
        """ Can utilize an index on the ol_delivery_d field. """
        return {
            'name': 'Orders',
            'predicate': None,
            'aggregate': [
                {
                    '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                },
                {
                    '$unwind': {'path': '$o_orderline'}
                },
                {
                    '$lookup': {'from': 'Item',
                                'localField': 'o_orderline.ol_i_id',
                                'foreignField': 'i_id',
                                'as': 'item'}
                },
                {
                    '$project': {
                        'ol_amount_pr': {
                            '$switch': {
                                'branches': [{
                                    'case': {'$regexMatch': {'input': {'$first': '$item.i_data'}, 'regex': '/^pr/'}},
                                    'then': '$o_orderline.ol_amount'
                                }],
                                'default': 0
                            }
                        }
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'ol_amount_sum_pr': {'$sum': '$ol_amount_pr'},
                        'ol_amount_sum': {'$sum': '$o_orderline.ol_amount'}
                    }
                },
                {
                    '$project': {
                        'promo_revenue': {
                            '$divide': [
                                {'$multiply': [100.0, '$ol_amount_pr']},
                                {'$add': [1, "$ol_amount_sum"]}
                            ]
                        }
                    }
                }
            ]
        }
