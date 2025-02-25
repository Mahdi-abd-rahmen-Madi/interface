import pstats

# Load the profiling results
stats = pstats.Stats('profile_output.log')

# Print a summary of the profiling results
stats.sort_stats('cumulative').print_stats(20)  # Show top 20 functions by cumulative time