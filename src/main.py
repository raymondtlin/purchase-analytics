from collections import deque, namedtuple, Counter
from csv import reader, writer
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


def lookup_merge(obj, lkp, lkp_value, on):
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
                lkp_dict = dict({lkp_value: lkp.get(getattr(record, on))})
            except LookupError as e:
                print(e.args, e.__annotations__)
            finally:
                yield {**record_dict, **lkp_dict}
    else:
        raise TypeError('Value passed into lkp argument is not a dictionary.')

    assert isinstance(obj, Csv)
    assert isinstance(lkp, dict)


# Get project root as Pathlib.path

rd = get_project_root()

# Create paths to instantiate Csv objects
op_file = rd.joinpath('input', 'order_products.csv')
pd_file = rd.joinpath('input', 'products.csv')

# Create mapping dict between product_id : department_id
products = Csv(pd_file)
dept_lkp = create_lookup(products, 'product_id', 'department_id')

# Instantiate the Csv objects
orders = Csv(op_file)
merged = lookup_merge(orders, dept_lkp, 'department_id', on='product_id')

# initialize containers for Aggregates
dq_orders = deque()
dq_ft_orders = deque()
dql = list()
dct = {}

for row in merged:
    dq_orders.append(row['department_id'])
    if row['reordered'] == '0':
        dq_ft_orders.append(row['department_id'])

for x in map(Counter, [dq_orders, dq_ft_orders]):
    dql.append(dict(x))

for k in dql[0].keys():
    dct[k] = tuple(d[k] for d in dql)

out_headers = ('department_id', 'number_of_orders', 'number_of_first_time_orders', 'percentage')


def csv_write(fname, headers, data):
    with rd.joinpath('output', fname).open('w', newline='\n') as csv:
        w = writer(csv, delimiter=',', lineterminator='\n')
        w.writerow(headers)
        for i, (k, v) in enumerate(sorted(data.items(), key=lambda x: int(x[0]))):
            w.writerow(tuple((k, v[0], v[1], '{:.2f}'.format(v[1] / v[0]))))


csv_write('report.csv', out_headers, dct)
