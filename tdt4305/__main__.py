import argparse
from itertools import chain

import tdt4305.part2.part_2 as part_2


# Parse command line arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--action', choices=('run', 'export-csv', 'export-tsv', 'export-txt'), default='run', help=" ")
parser.add_argument('--sample', action="store_true", help=" ")

# parser.print_help()
args = parser.parse_args()
action = args.action
sample = args.sample

part_2.main(action, sample)


