from collections import defaultdict, namedtuple, Counter
from csv import reader
from pathlib import PosixPath


class Csv(object):
    """
    Iterable container which generates named tuples for each read row from a CSV file

    Attributes:
        file_path (PosixPath): posix-path object for file to read
        reader (Reader): csv.reader object iterable
        fields (list):  list of column names
    """

    def __init__(self, file_path):
        """
        Constructor for Csv class
        :param file_path: pathlib.PosixPath object specifying file to read
        """
        self.file_path = file_path

        # Sanitizing input, if the parameter object is PosixPath, we can use pathlib.open \
        # If not, we will use vanilla open

        if isinstance(self.file_path, PosixPath):
            self.reader = reader(self.file_path.open())
        else:
            self.reader = reader(open(file_path.as_posix(), 'r'))

        self.fields = list(next(self.reader))

    def __iter__(self):
        self._length = 0
        self._counter = Counter()

        yield from self.reader

    def parse_record(self):
        """
        Returns single row entry as a named tuple
        :return: named tuple for a single row
        """
        Record = namedtuple('Record_', self.fields)

        for row in map(Record._make, self.reader):
            yield row


def ifilter(predicate, iterable):
    if predicate is None:
        predicate = bool
    for x in iterable:
        if predicate(x):
            yield x

#  This function returns the product to department mapping.


def map_product_departments():
    """
    Returns a sorted dictionary mapping product_id to department_id
    """
    products = CsvData('products.csv')
    pd = defaultdict()
    pd = {row['product_id']: row['department_id'] for row in products.__iter__()}
    pd = sorted(pd.items(), key=lambda x: int(x[0]))
    return pd


def merge_join():
    """
    Returns an iterable with a SQL-esque join on product_id between the two streams.
    """
    orders = CsvData('order_products.csv')
    for row in orders.__iter__():
        for i, (k, v) in enumerate(map_product_departments()):
            yield(ifilter(lambda x: k == x, row['product_id']), (v, row))


agg_orders = defaultdict(int)
ft = defaultdict(int)

#  Defaultdict allows us to create an aggregated table of orders by
#  department.

for i, (k, d) in merge_join():
    agg_orders[k] += 1

# Couldn't solve how to filter the iterable output of the merged stream by
# "reordered" so I decided aggregate new orders by product_id, and then join
#  department_id to product_id and sum(count) by department_id.  The sum part
#  is still missing however.

new_orders = []
for row in CsvData('products.csv').get_subset('reordered','0'):
    new_orders.append(row['product_id'])

new_orders_cnt = dict(Counter(new_orders))

for d in new_orders_cnt:
    for i, (product_id, dept_id) in enumerate(map_product_departments()):
        if product_id == d[0]:
            d.update(dict(department_id=dept_id))


with open(root.joinpath('output', 'report.csv'), 'w', newline='\n') as outFile:
    field_names = ['department_id', 'number_of_orders', 'number_of_first_time_orders', 'percentage']
    data = [agg_orders[0], agg_orders[1], new_orders_cnt[1], {}.format('d', new_orders_cnt[1]/agg_orders[1], '.2f')]