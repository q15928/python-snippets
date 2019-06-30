from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pyarrow.parquet as pq
import pandas as pd
import argparse

def pq_to_csv(input_path, output_path):

    df = pq.read_table(input_path).to_pandas()
    df.to_csv(output_path, index=False)

def main():
    parser = argparse.ArgumentParser("Convert parquet to csv")
    parser.add_argument("-i", "--input", dest="input", default=False, help="input path for parquet file")
    parser.add_argument("-o", "--output", dest="output", default=False, help="output path for csv file")
    args = parser.parse_args()
    
    if (not args.input or not args.output):
        print("please specify input and output path")
        return
    
    try:
        pq_to_csv(args.input, args.output)
    except Exception as e:
        print("error - ", e)


if __name__ == "__main__":
    main()