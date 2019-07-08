from csv import DictReader
from operator import eq
import pathlib

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

    def generate(self):
        """
        :return: DictReader object as generator
        """
        yield from self.reader

    def get_keys(self) -> list:
        """
        :return list of column names
        """
        return self.headers

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
                yield r if eq(r[predicate_column], predicate_expression) else {}

    def get_values(self, expression):

        if expression not in self.get_keys():
            raise KeyError('Column does not exist in keys')
        else:
            try:
                return [row[expression] for row in self.generate()]
            except BaseException as e:
                print(e.args, e.__traceback__)


def summer(values):
    if values is None:
        raise BaseException(print('Empty list'))
    try:
        return dict(Counter(value for value in values))
    except BaseException as e:
        print(e.args, e.__traceback__)
        
