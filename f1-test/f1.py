import pandas as pd
from sklearn.metrics import f1_score


def read_file(file_path):
    """Read the file into DataFrame"""
    df = pd.read_csv(file_path, sep='|', skiprows=1, index_col=0,
                     parse_dates=True)
    return df


def compute_f1_score(df):
    """Compute the F1 score"""
    return f1_score(df["y"].values, df["yhat"].values)


if __name__ == "__main__":
    file_path = "./data/test.tar.gz"
    df = read_file(file_path)

    # Filter the DataFrame which contains Thursday's data only
    df_thur = df[df.index.weekday_name == "Thursday"]

    # Compute F1 score
    f1 = compute_f1_score(df_thur)

    # Print the result
    print("F1 score for Thursdays is {0:.3F}".format(f1))
