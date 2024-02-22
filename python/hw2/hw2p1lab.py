import collections
import time
from collections import Counter

def sorted_word_frequency(lines: list[str]) -> dict[str, int]:
    sorted_frequency = dict()
    for line in lines:
        line = line.strip().lower()
        items = line.split()

        for item in items:
            if item in sorted_frequency:
                sorted_frequency[item] = sorted_frequency[item] + 1
            else:
                sorted_frequency[item]=1
    sorted_frequency=collections.OrderedDict(sorted(sorted_frequency.items(),key=lambda kv:kv[1],reverse=True))

    return  sorted_frequency


def print_top_words(file_path: str) -> None:
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()


            # Call sorted_word_frequency function to get word frequencies
            word_frequency = sorted_word_frequency(lines)
            top_words=list(word_frequency.items())[:10]
            num_lines = len(lines)
            print(f"Number of lines: {num_lines}")

            # Print the top 10 words by frequency
            for word, freq in top_words:
                print(f"{word}: {freq}")

    except FileNotFoundError:
        print(f"File '{file_path}' not found.")


# Command-line execution
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python hw2p1.py input_file_path")
    else:
        file_path = sys.argv[1]
        # Measure the execution time of the function
        start_time = time.time()  # Record the current time

        print_top_words(file_path)

        end_time = time.time()  # Record the current time again
        # Calculate the elapsed time
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time} seconds")
print_top_words('/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/input/alice_in_wonderland.txt')