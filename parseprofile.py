import pstats
import sys

# Load and analyze profiling data
def parse_profile(file_path="profile_output.prof"):
    stats = pstats.Stats(file_path)
    # stats.sort_stats("cumulative")  # Sort by cumulative time
    stats.sort_stats("tottime")  # Sort by total time in a given function
    stats.print_stats(100)  # Print top N results

if __name__ == "__main__":
    for file_name in sys.argv[1:]:
        # print(file_name)
        parse_profile(file_path=file_name)

