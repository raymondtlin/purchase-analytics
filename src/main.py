from csv import DictReader
from operator import eq
import pathlib
from collections import Counter
from src.utils import get_project_root

class CsvData(object):
    """
    Container for DictReader objects
    """
    def __init__(self, fname):
        """
        Instantiates a DictReader object from the specified filename.
        :param fname: string with file extension.
        """
        root = get_project_root()
        self.path = root.joinpath('input', fname)
        self.reader = DictReader(self.path.open(encoding='utf-8'))
        self.headers = self.reader.fieldnames

    def __iter__(self):
        """
        :return: DictReader object as generator
        """
        yield from self.reader
    
    def get_keys(self) -> list:
        """
        :return list of column names
        """
        return self.headers

    def length(self, iterable):
        return iter(iterable).__len__
    
	
    def get_subset(self, predicate_column, predicate_expression):
        """
        Lazy evaluation that returns the first column for every row where the predicate equals the comparator
        :param predicate_column: column value in instance to compare
        :param predicate_expression: value to compare against predicate_column
        :return: 
        """

        if predicate_column not in self.get_keys():
            raise KeyError
        else:
            for r in self.generate():
                if eq(r[predicate_column], predicate_expression):
			yield r

    def get_values(self, expression):

        if expression not in self.get_keys():
            raise KeyError('Column does not exist in keys')
        else:
            try:
                return [row[expression] for row in self.generate()]
            except BaseException as e:
                print(e.args, e.__traceback__)
    
    
    def restart(self,reset=bool):
        if reset:
            self.__init__(self.fname)
        

def ifilter(predicate, iterable):
    if predicate is None:
        predicate = bool
    for x in iterable:
        if predicate(x):
            yield x
            
def map_product_departments():
    """
    Returns a sorted dictionary mapping product_id to department_id
    """
    products = CsvData('products.csv')
    pd = defaultdict()
    pd = {row['product_id']:row['department_id'] for row in products.__iter__()}
    pd = sorted(pd.items(),key=lambda x: int(x[0]))
    return pd

def merge_join():
	orders = CsvData('orders.csv')
    for row in orders.__iter__():
        for i, (k,v) in enumerate(map_product_departments()):
            yield(ifilter(lambda x: k == x, row['product_id']), (v,row))
            
            
agg_orders = defaultdict(int)
ft = defaultdict(int)

for i,(k,d) in merge_join(CsvData('orders.csv')):
    agg_orders[k] += 1
    
new_orders = []
for row in CsvData('products.csv').get_subset('reordered','0')
	new_orders.append(row['product_id'])

new_orders_cnt = dict(Counter(new_orders))

for d in new_orders_cnt:
	for i,(product_id, dept_id) in enumerate(map_product_departments()):
		if product_id == d[0]:
			d.update(dict(department_id=dept_id))

with open(root.joinpath('output','report.csv'),'w',newline='\n') as outFile:
	field_names = ['department_id','number_of_orders','number_of_first_time_orders','percentage']
	
	data = [agg_orders[0],agg_orders[1],new_orders_cnt[1],{}.format('d', new_orders_cnt[1]/agg_orders[1], '7.2f')]
	
	
	
