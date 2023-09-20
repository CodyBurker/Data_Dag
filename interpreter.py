import regex as re
import polars as pl
from enum import Enum
from abc import ABC, abstractmethod
from commands import base_funcs, CommandType
import logging



class InterpreterState(Enum):
    INIT = 1
    MOD = 2

def get_command(line):
    # Parse first command
    command = re.search(r'^\s*(?P<command>\w+)', line).group('command')
    return command

class Interpreter():
    def set_logging(self, level):
        logging.basicConfig(level=level)

    def __init__(self) -> None:
        self.current_df = None
        self.env = {}
        return None

    def run(self, input_string):
        # Split input string into lines
        lines = input_string.split('\n')
        # Remove comments and lines with only whitespace
        lines = [line for line in lines if not line.strip().startswith('#') and line.strip()]
        # Create dictionary of lines with line number as key
        line_dict = {i: line for i, line in enumerate(lines)}

        self.state = InterpreterState.INIT

        for line_no in line_dict:
            line = line_dict[line_no]
            # Skip empty lines
            if not line.strip():
                continue
            # Get command
            command = get_command(line)

            # Check if command is in base_funcs
            if command in base_funcs:
                # Get Command
                func = base_funcs[command]
                logging.debug(f'Line {line_no}: {line}')
                logging.debug(f'Current state: {self.state}')
                logging.debug(f'Command : {command} of type {func.command_type}')
                if self.state == InterpreterState.INIT and func.command_type == CommandType.INITIAL:
                    self.current_df = func.execute(line, None, self.env)
                    self.state = InterpreterState.MOD
                    logging.debug(f'\tChanging state to {self.state}')
                elif self.state == InterpreterState.MOD and func.command_type == CommandType.MODIFYING:
                    self.current_df = func.execute(line, self.current_df, self.env)
                    pass
                elif self.state == InterpreterState.MOD and func.command_type == CommandType.TERMINAL:
                    self.current_df = func.execute(line, self.current_df, self.env)
                    self.state = InterpreterState.INIT
                    logging.debug(f'\tChanging state to {self.state}')
                else:
                    # Raise error
                    print(f'Error on line {line_no}: Invalid command {command}. Not expecting a command of type {func.command_type}')
                    raise Exception()
            else:
                print(f'Error on line {line_no}: Invalid command {command}. Try one of {list(base_funcs.keys())}')