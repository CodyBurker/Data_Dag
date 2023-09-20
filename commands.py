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
    # Init
    def __init__(self, command_type: CommandType, command_text) -> None:
        self.command_type = command_type
        self.command_text = command_text
        return None
    
    def set_function(self, func):
        self.func = func
        return self

    
def parse_select(line, input_df):
    # Given a select statement line, parse it
    # Column names are surrounded by {} and separated by one or more spaces
    # Example: select {col1} {col2} {col3}
    col_names = re.findall(r'{([\w\. \d]+)}', line)
    return input_df.select(col_names)

select = Command(CommandType.MODIFYING, command_text="select").set_function(parse_select)



def parse_read_csv(line):
    # Given a read statement line, parse it
    # Example: read "file.csv"
    file_name = re.search(r'read_csv\s*"(?P<file_name>.+)"', line).group('file_name')
    return pl.scan_csv(file_name)

read_csv = Command(CommandType.INITIAL, command_text="read_csv").set_function(parse_read_csv)

def parse_filter(line, input_df):
    # Given a filter statement line, parse it
    # Example: filter {col} > 10 & {col} < 20 |
    # Need to convert these to valid polars syntax
    
    #Extract everything after filter and before | if it exists
    # By replacing 'filter' and '|' with empty string
    filter_str = re.sub(r'filter\s*|\s*\|', '', line)
    # Now replace {col} with pl.col('col')
    filter_str = re.sub(r'{([\w\. \d]+)}', r'pl.col("\1")', filter_str)
    parsed_filter = eval(filter_str)
    try:
        filtered_df = input_df.filter(parsed_filter)
    except:
        print("Error: Invalid filter")
        return input_df
    return filtered_df

filter = Command(CommandType.MODIFYING, command_text="filter").set_function(parse_filter)

def parse_show(input_df):
    # Given a show statement line, parse it
    # Example: show
    display(
        input_df.limit(10).collect()
    )

show = Command(CommandType.TERMINAL, command_text="show").set_function(parse_show)

def parse_add_column(line, input_df):
    # Given an add column statement line, parse it
    # Example: add_column {col3} = {col1} + {col2}
    # Need to convert these to valid polars syntax
    new_col_name = re.search(r'add_column\s*{([\w\. \d]+)}\s*=\s*', line).group(1)
    #Extract everything after =
    # By replacing 'add_column' with empty string
    add_col_str = re.sub(r'add_column\s*{[\w\. \d]+}\s*=\s*', '', line)
    # Now replace {col} with pl.col('col')
    add_col_str = re.sub(r'{([\w\. \d]+)}', r'pl.col("\1")', add_col_str)
    parsed_add_col = eval(add_col_str)
    new_df = input_df.with_columns(parsed_add_col.alias(new_col_name))
    return new_df

add_column = Command(CommandType.MODIFYING, command_text="add_column").set_function(parse_add_column)

def parse_group_by(line, input_df):
    # Given a group by statement line, parse it
    # Example: group_by {col1} {col2}
    # Need to convert these to valid polars syntax
    group_by_str = re.sub(r'group_by\s*', '', line)
    # Replace multiple spaces with single space
    group_by_str = re.sub(r'\s+', ' ', group_by_str)
    # Extract column names as list
    group_by_cols = re.findall(r'{([\w\. \d]+)}', group_by_str)
    grouped_df = input_df.group_by(*group_by_cols)
    return grouped_df

group_by = Command(CommandType.MODIFYING, command_text="group_by").set_function(parse_group_by)

def parse_summarize(line, input_df):
    # Given a summarize statement line, parse it
    # Example: summarize {col1} = sum({col2})
    # Replace any function of the form func(...) with pl.func(...)
    summarize_str = re.sub(r'\s*summarize\s*', '', line)
    # Split on commas to get each new column
    summarize_str = re.split(r'\s*,\s*', summarize_str)
    aggregated_columns = []
    for new_col in summarize_str:
        # Get the new column name
        new_col_name = re.search(r'{([\w\. \d]+)}\s*=', new_col).group(1)
        # Get the function name
        # E.g. sum({col1}) -> sum
        func_name = re.search(r'(\w+)\(\{', new_col).group(1)
        # Get the column name being aggregated
        # E.g. sum({col1}) -> col1
        col_name = re.search(r'\{([\w\. \d]+)\}', new_col).group(1)
        # Rewrite as pl.col('col1').func().alias(new_col_name)
        new_col_str = f'pl.col("{col_name}").{func_name}().alias("{new_col_name}")'
        # Evaluate the string to get the new column
        new_col = eval(new_col_str)
        aggregated_columns.append(new_col)
    # Aggregate the columns
    new_df = input_df.agg(*aggregated_columns)
    return new_df
summarize = Command(CommandType.MODIFYING, command_text="summarize").set_function(parse_summarize)

base_funcs_raw = [read_csv, select, filter, add_column, group_by, summarize, show]

# Create dictionary with command as key, class as value

base_funcs = {x.command_text: x for x in base_funcs_raw}