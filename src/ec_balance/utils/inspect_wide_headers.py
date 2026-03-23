# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Utility – vytiskni hlavičky wide CSV")
    ap.add_argument("--path", required=True)
    ap.add_argument("--rows", type=int, default=3)
    ap.add_argument("--sep", default=",")
    args = ap.parse_args()

    df = pd.read_csv(args.path, sep=args.sep, nrows=args.rows)
    print(df.head(args.rows))

if __name__ == "__main__":
    main()
