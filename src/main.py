from collections import defaultdict, namedtuple, Counter
from csv import reader
from pathlib import PosixPath

from src.utils import get_project_root


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


def create_lookup(obj, key, val):
    """
    Creates a mapping between a key and value from the given instance object
    :param obj: instance of Csv class
    :param key: field in obj
    :param val: field in obj
    :return: ordered mapping
    """
    cols = {key, val}

    if not isinstance(obj, Csv):
        raise TypeError('obj argument is not an instance of Csv')
    else:
        if len(set(obj.fields) & cols) < 2:
            raise LookupError('Unable to find argument(s):{}'.format(i for i in {cols - set(obj.fields)}))
        else:
            d = {getattr(t, key): getattr(t, val) for t in obj.parse_record()}
            return dict(sorted(d.items(), key=lambda x: int(x[0])))

    assert len(cols & set(obj.fields)) == 2
    assert isinstance(obj, Csv)


def lookup_merge(obj, lkp, lkp_value, on=""):
    """
    Generates a merged dictionary by looking up the corresponding key's value in a dictionary
    :param obj: instance of Csv
    :param lkp: mapping of key:value
    :param lkp_value: name for value to merge
    :param on: shared column in both obj and lkp
    :return: merged dict
    """
    if isinstance(lkp, dict):
        for record in obj.parse_record():
            record_dict = record._asdict()
            try:
                lkp_dict = dict(lkp_value=lkp.get(getattr(record, on)))
            except LookupError as e:
                print(e.args, e.__annotations__)
            finally:
                yield {**record_dict, **lkp_dict}
    else:
        raise TypeError('Value passed into lkp argument is not a dictionary.')

    assert isinstance(obj, Csv)
    assert isinstance(lkp, dict)
    assert on in lkp.keys()


# Get project root as Pathlib.path

rd = get_project_root()

# Create paths to instantiate Csv objects
op_file = rd.joinpath('input', 'order_products.csv')
pd_file = rd.joinpath('input', 'products.csv')

# Instantiate the Csv objects
orders = Csv(op_file)
products = Csv(pd_file)

# Create mapping dict between product_id : department_id
dept_lkp = create_lookup(products, 'product_id', 'department_id')

merged = lookup_merge(orders, dept_lkp, 'department_id', on='product_id')

for row in merged:
    dict(department_id=row[department_id],
         num_of_orders=count(row['department_id']),
         number_of_first_orders=1 - row['reordered'],
         percentage=format((1 - row['reordered']) / count(row['department_id']), 'd', 2)
         )


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