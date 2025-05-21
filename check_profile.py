import pstats
from pstats import SortKey

p = pstats.Stats('output.prof')
p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(20)  # Prints top 20 cumulative time
p.strip_dirs().sort_stats(SortKey.TIME).print_stats(20)  # Prints top 20 total time
