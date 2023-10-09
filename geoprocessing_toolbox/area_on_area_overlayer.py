import geopandas as gpd
import pandas as pd
from dask import delayed

# client = Client(n_workers=4, threads_per_worker=4)


def overlay_group_by(
    df1: gpd.GeoDataFrame,
    df2: gpd.GeoDataFrame,
    attribute1: str,
    attribute2: str,
    how: str = "intersection",
    lazy: bool = True,
) -> gpd.GeoDataFrame:
    """This function is a wrapper of the 'overlay' function of the geopandas library. It allows to perform the overlay
    of two GeoDataFrames by grouping them by two attributes. The function is lazy by default,
    which means that the computation is not performed until the 'compute' function is called.
    This function is useful when the overlay is performed on a large number of groups.

    Parameters
    ----------
    df1 : gpd.GeoDataFrame
        Input GeoDataFrame 1
    df2 : gpd.GeoDataFrame
        Input GeoDataFrame 1
    attribute1 : str
        Attritbute of the GeoDataFrame 1 used for grouping
    attribute2 : str
        Attritbute of the GeoDataFrame 2 used for grouping
    how : str, optional
        Method of spatial overlay: "intersection", "union", "identity",
        "symmetric_difference" or "difference"., by default "intersection"
    lazy : bool, optional
        I

    Returns
    -------
    gpd.GeoDataFrame
        _description_
    """

    input_df1 = df1.copy()
    input_df2 = df2.copy()

    # Define indexes
    if attribute1 not in input_df1.index.names:
        input_df1.set_index(attribute1, inplace=True)

    if attribute2 not in input_df2.index.names:
        input_df2.set_index(attribute2, inplace=True)

    # Define groups uniques values
    unique_value_df1 = list(input_df1.index.unique())
    unique_value_df2 = list(input_df2.index.unique())
    unique_value_df1_df2 = list(set(unique_value_df1 + unique_value_df2))

    results = []
    for group in unique_value_df1_df2:
        if group in unique_value_df1:
            df1_group = input_df1.loc[[group]]
        else:
            df1_group = pd.DataFrame()

        if group in unique_value_df2:
            df2_group = input_df2.loc[[group]]
        else:
            df2_group = pd.DataFrame()

        if not df1_group.empty and not df2_group.empty:
            # Use the 'delayed' function to make the 'overlay' computation lazy
            if lazy:
                result = delayed(gpd.overlay)(df1_group, df2_group, how=how)
            else:
                result = gpd.overlay(df1_group, df2_group, how=how)
            results.append(result)

        # When all results are ready, concatenate them
        if lazy:
            merged_df = delayed(pd.concat)(results, ignore_index=True)

    if lazy:
        output_df = merged_df.compute()
    else:
        output_df = pd.concat(results, ignore_index=True)

    return output_df
