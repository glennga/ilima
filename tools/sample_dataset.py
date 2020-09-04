import argparse
import subprocess
import random

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sample N lines from a large file.')
    parser.add_argument('input', type=str, help='File to sample from.')
    parser.add_argument('output', type=str, help='File to output to.')
    parser.add_argument('n', type=int, help='Number of lines to sample.')
    parser_args = parser.parse_args()

    # Get total number of lines.
    total_number_of_lines = 0
    subprocess_pipe = subprocess.Popen(
        ['wc', '-l', parser_args.input],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    for stdout_line in iter(subprocess_pipe.stdout.readline, ""):
        total_number_of_lines = int(stdout_line.strip().split()[0])
        break
    print(f'Total number of lines: {total_number_of_lines}.')
    line_number_samples = set(random.sample(range(0, total_number_of_lines), parser_args.n))

    print(f'Now performing sample...')
    output_fp = open(parser_args.output, 'w')
    with open(parser_args.input, 'rt') as input_fp:
        line_counter = 0
        for line in input_fp:
            if line_counter in line_number_samples:
                output_fp.write(line)
            line_counter = line_counter + 1

    print(f'Finished sampling.')
    output_fp.close()
