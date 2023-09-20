import regex as re
import polars as pl
from enum import Enum
from IPython.display import display

from abc import ABC, abstractmethod

# Enum for command types
class CommandType(Enum):
    INITIAL = 1
    MODIFYING = 2
    TERMINAL = 3

# Base class for commands
class Command(ABC):
    command_type: CommandType
    command_text: str
    def __init__(self) -> None:
        return None
    
    @abstractmethod
    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        return input_df

class select(Command):

    command_type = CommandType.MODIFYING
    command_text = "select"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        col_names = re.findall(r'{([\w\. \d]+)}', line)
        # return input_df.select(col_names)
        return None # Test for github action automated unit testing
    
class read_csv(Command):

    command_type = CommandType.INITIAL
    command_text = "read_csv"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        file_name = re.search(r'read_csv\s*"(?P<file_name>.+)"', line).group('file_name')
        return pl.scan_csv(file_name)

class filter(Command):

    command_type = CommandType.MODIFYING
    command_text = "filter"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        filter_str = re.sub(r'filter\s*|\s*\|', '', line)
        filter_str = re.sub(r'{([\w\. \d]+)}', r'pl.col("\1")', filter_str)
        parsed_filter = eval(filter_str)
        try:
            filtered_df = input_df.filter(parsed_filter)
        except:
            print("Error: Invalid filter")
            return input_df
        return filtered_df

class show(Command):
    command_type = CommandType.TERMINAL
    command_text = "show"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        display(
            input_df.limit(10).collect()
        )
        return None

class add_column(Command): 
    command_type = CommandType.MODIFYING
    command_text = "add_column"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        new_col_name = re.search(r'add_column\s*{([\w\. \d]+)}\s*=\s*', line).group(1)
        #Extract everything after =
        # By replacing 'add_column' with empty string
        add_col_str = re.sub(r'add_column\s*{[\w\. \d]+}\s*=\s*', '', line)
        # Now replace {col} with pl.col('col')
        add_col_str = re.sub(r'{([\w\. \d]+)}', r'pl.col("\1")', add_col_str)
        parsed_add_col = eval(add_col_str)
        new_df = input_df.with_columns(parsed_add_col.alias(new_col_name))
        return new_df

class group_by(Command):
    command_type = CommandType.MODIFYING
    command_text = "group_by"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        group_by_str = re.sub(r'group_by\s*', '', line)
        group_by_str = re.sub(r'\s+', ' ', group_by_str)
        group_by_cols = re.findall(r'{([\w\. \d]+)}', group_by_str)
        grouped_df = input_df.group_by(*group_by_cols)
        return grouped_df

class summarize(Command):
    command_type = CommandType.MODIFYING
    command_text = "summarize"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        summarize_str = re.sub(r'\s*summarize\s*', '', line)
        summarize_str = re.split(r'\s*,\s*', summarize_str)
        aggregated_columns = []
        for new_col in summarize_str:
            new_col_name = re.search(r'{([\w\. _\d]+)}\s*=', new_col).group(1)
            func_name = re.search(r'(\w+)\(\{', new_col).group(1)
            col_name = re.search(r'\(\{([\w\. \d]+)\}\)', new_col).group(1)
            new_col_str = f'pl.col("{col_name}").{func_name}().alias("{new_col_name}")'
            new_col = eval(new_col_str)
            aggregated_columns.append(new_col)
        new_df = input_df.agg(*aggregated_columns)
        return new_df

class save_variable(Command):
    command_type = CommandType.TERMINAL
    command_text = "save_variable"

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        # Parse variable name from line, after save_variable
        var_name = re.search(r'save_variable\s*([\w|\d|_|\.]+)', line).group(1)
        env[var_name] = input_df
        return None

class load_variable(Command):
    command_type = CommandType.INITIAL
    command_text = 'load_variable'

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        var_name = re.search(r'load_variable\s*([\w|\d|_|\.]+)', line).group(1)
        loaded_df = env[var_name]
        return loaded_df

class sort(Command):
    command_type = CommandType.MODIFYING
    command_text = 'sort'

    def execute(self, line: str, input_df: pl.DataFrame, env: dict):
        # Syntax is sort {col} desc, {col} asc
        sort_str = re.sub(r'sort\s*', '', line)
        sort_str = re.split(r'\s*,\s*', sort_str)
        sort_cols = []
        for col in sort_str:
            col_name = re.search(r'{([\w\. \d]+)}', col).group(1)
            sort_order = re.search(r'(asc|desc)', col).group(1)
            sort_cols.append((col_name, sort_order))
        # Create list of ascending/descending
        # with True if descending, False if ascending
        sort_order = [True if x[1] == 'desc' else False for x in sort_cols]
        # Create list of strings of column names
        sort_cols = [x[0] for x in sort_cols]
        sorted_df = input_df.sort(sort_cols, descending=sort_order)
        return sorted_df

base_funcs_raw = [
    select(),
    read_csv(),
    filter(),
    show(),
    add_column(),
    group_by(),
    summarize(),
    save_variable(),
    load_variable(),
    sort()
]

# Create dictionary with command as key, class as value

base_funcs = {x.command_text: x for x in base_funcs_raw}