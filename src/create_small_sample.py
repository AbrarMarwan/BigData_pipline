import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
import argparse
import itertools

def create_sample(input_path, output_path, num_rows):
    with open(input_path, mode='r', encoding='utf-8', errors='ignore') as infile:
        with open(output_path, mode='w', encoding='utf-8') as outfile:
            for line in itertools.islice(infile, num_rows + 1):  # +1 للـ Header
                outfile.write(line)
    print(f"Sample generated successfully: {output_path} with {num_rows} rows.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input dirty CSV")
    parser.add_argument("--output", default="data/sample_orders.csv", help="Path to output sample")
    parser.add_argument("--rows", type=int, default=5000, help="Number of rows")
    args = parser.parse_args()

    create_sample(args.input, args.output, args.rows)