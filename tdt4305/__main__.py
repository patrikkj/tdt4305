import argparse
from itertools import chain

import tdt4305.part1.part_1 as part_1


def parse_range(ranges):
    '''Used for parsing command line range arguments.'''
    return sorted(set(chain(*[_parse_range(range_) for range_ in ranges.split(',')])))

def _parse_range(range_):
    '''Used for parsing command line range arguments.'''
    try:
        start, *end = (int(i) for i in range_.split('-'))
        end = end[0] if end else start
    except ValueError as e:
        raise ValueError(f"Bad range: '{range_}'")
    return range(start, end + 1)


# Parse command line arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--action', choices=('run', 'export-csv', 'export-tsv', 'export-txt'), default='run', help=" ")
parser.add_argument('--tasks', default='1-6', help=" ")
parser.add_argument('--sample', action="store_true", help=" ")

parser.print_help()
args = parser.parse_args()
action = args.action
tasks = parse_range(args.tasks)
sample = args.sample

part_1.main(action, tasks, sample)


