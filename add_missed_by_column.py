import pandas as pd
import numpy as np


def add_missed_category(
    df,
    current_col,
    target_col,
    output_col='Missing_Next_Club'
):
    """
    Adds a column to `df` categorizing how far each row's `current_col`
    value falls short of `target_col`, as a percentage band.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe (modified in place, and also returned).
    current_col : str
        Column name holding the current/actual value.
    target_col : str
        Column name holding the target value to compare against.
    output_col : str, default 'Missing_Next_Club'
        Name of the new column to write the category into.

    Returns
    -------
    pd.DataFrame
        The same dataframe, with `output_col` added/overwritten.
    """
    current = pd.to_numeric(df[current_col], errors='coerce')
    target = pd.to_numeric(df[target_col], errors='coerce')

    missed_pct = (target - current).div(target).mul(100)

    df[output_col] = np.select(
        [
            current.isna() | target.isna() | target.eq(0),
            current.ge(target),
            missed_pct.le(10),
            missed_pct.le(20),
            missed_pct.le(30),
            missed_pct.le(40),
            missed_pct.le(50),
            missed_pct.le(60),
            missed_pct.le(70)
        ],
        [
            'Not Applicable',
            'Achieved or Above Target',
            'Missing by upto 10%',
            'Missing by upto 20%',
            'Missing by upto 30%',
            'Missing by upto 40%',
            'Missing by upto 50%',
            'Missing by upto 60%',
            'Missing by upto 70%'
        ],
        default='Missing by >70%'
    )

    return df


if __name__ == '__main__':
    # Quick sanity check with sample data
    sample = pd.DataFrame({
        'Current': [100, 95, 80, 50, 0, None, 120],
        'Target':  [100, 100, 100, 100, 100, 100, 0],
    })

    result = add_missed_category(sample, 'Current', 'Target')
    print(result)
