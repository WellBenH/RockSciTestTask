#!/usr/bin/env python
# encoding: utf-8

"""Test task for RockSci from Vadim F."""

import os
try:
    from typing import Union, Any, Callable, Dict, List, Tuple
    import pandas as pd
except ModuleNotFoundError as e:
    if e.name == 'typing':
        print('Types have been introduced in Python '
              'from version 3.5. Your version is outdated! Time2upd!')
    elif e.name == 'pandas':
        print('Use "pip install -r requirements.txt" in the root directory')
    import sys
    sys.exit(-1)


def try_convert_date_time(arg: Union[pd.core.api.Timestamp, str, int]) \
        -> Union[pd.core.api.Timestamp, bool]:
    """
    The function to convert data to date.

    It returns arg if this is a Timestamp obj or attempts to convert
    from string or int to Timestamp.
    Function returns a False in a case of failure.

    :param arg: Timestamp, str 8 ch w/o sep or 10 ch w/ sep,
    int (8 dig) to convert. Date format if YYYYMMDD.
    :return: Timestamp if successful otherwise False.
    """

    if type(arg) == pd.core.api.Timestamp:
        return arg

    arg = str(arg)
    for symbol in set(ch for ch in arg if not ch.isdigit()):
        arg = arg.replace(symbol, '')
    try:
        return pd.to_datetime(arg, format='%Y%m%d')
    except ValueError:
        return False


def date_checker(func: Callable[..., pd.Series]) -> Any:
    """
    The function is decorator for check date.

    It attempts to get Timestamps and call function with new
    converted arguments otherwise raise Exception.

    :param func: function for decorate.
    :return: result of function.
    """

    def wrapper(*args, **kwargs):
        new_arg = [args[0]]  # self in the list
        for arg in args[1:]:
            date = try_convert_date_time(arg)
            if not date:
                raise TypeError('Incorrect format of date, use:\n'
                                '1)int - 8 digits\n'
                                '2)str - 8+ (with/without seps) characters'
                                ' (YYYYMMDD) with any separators')

            new_arg.append(date)
        return func(*new_arg, **kwargs)
    return wrapper


def df_checker(func: Callable[..., Union[pd.DataFrame, pd.Series, None]])\
        -> Any:
    """
    The function is decorator for check DataFrame object.

    If there is no necessary argument (DataFrame) in the args of
    function, then it will not call the function and will return None
    otherwise will call function.

    :param func: function for decorate.
    :return: result of function.
    """

    def wrapper(*args, **kwargs):
        for arg in args:
            if type(arg) in (pd.DataFrame, pd.Series):
                try:
                    return func(*args, **kwargs)
                except KeyError:
                    return None
        return None
    return wrapper


class FormalData:
    """First parent class.
    Data from files and some fields are stored here.
    """

    def __init__(self) -> None:
        self.__df_raw_dict = {}
        self.__df_asset = None
        self.__df_currency = None
        self.__raw_currency = None
        self.__df_total = None

    @property
    def _df_raw(self) -> Dict:
        return self.__df_raw_dict

    @_df_raw.setter
    def _df_raw(self, df_value: Dict) -> None:
        self.__df_raw_dict = df_value

    @property
    def _df_asset(self) -> Union[pd.DataFrame, None]:
        return self.__df_asset

    @_df_asset.setter
    def _df_asset(self, asset_value: pd.DataFrame) -> None:
        self.__df_asset = asset_value

    @property
    def _df_currency(self) -> Union[pd.DataFrame, None]:
        return self.__df_currency

    @_df_currency.setter
    def _df_currency(self, currency_value: pd.DataFrame) -> None:
        self.__df_currency = currency_value

    @property
    def _df_currency_raw(self) -> Union[pd.DataFrame, None]:
        return self.__raw_currency

    @_df_currency_raw.setter
    def _df_currency_raw(self, currency_raw_value: pd.DataFrame) -> None:
        self.__raw_currency = currency_raw_value

    @property
    def _df_total(self) -> Union[pd.DataFrame, None]:
        return self.__df_total

    @_df_total.setter
    def _df_total(self, total_value: pd.DataFrame) -> None:
        self.__df_total = total_value

    @staticmethod
    @df_checker
    def __get_an_attitude(df: pd.DataFrame) -> pd.DataFrame:
        """
        The method calculates (df[n]-df[n-1])/df[n-1].

        The attitude is needed to calculate R[i,t], CR[i,t], TR[i,t].

        :param df: DataFrame for calculating with attitude
        (R[i,t], CR[i,t], TR[i,t]).
        :return: Calculated DateFrame.
        """
        attitude = df.diff().div(df.shift(1))
        return attitude

    @property
    def __get_currency_raw(self) -> Union[pd.DataFrame, None]:
        """
        The method is needed to get currencies DataFrame obj (table)

        Two DataFrame tables (currencies and exchanges) merged here.
        If any of the tables is not available method return None.
        Columns of new table are asset names from currencies,
        indexes are dates from exchanges.

        :return: DataFrame - asset table with exchange rates or None.
        """

        currency_df = self._df_raw.get('currencies')
        exchanges_df = self._df_raw.get('exchanges')
        if currency_df is None or exchanges_df is None:
            return None
        # merge transpose exchange on currency on right key currency
        # (like a SQL right join) with drop currency row from result
        merge_df = exchanges_df.T.merge(
            currency_df, how='right', right_on='currency',
            left_index=True).T.drop('currency')
        merge_df.index = pd.to_datetime(merge_df.index)
        # get columns with all nan to fill them with 1
        # it is assumed that this is a currency column to which other
        # or equivalent currency is converted with
        # a conversion factor of 1
        new_columns = merge_df.columns[merge_df.isna().all()].tolist()
        merge_df[new_columns] = merge_df[new_columns].fillna(value=1)
        return merge_df

    @property
    def __get_total_raw(self) -> Union[pd.DataFrame, None]:
        """
        The method is needed to get total DataFrame obj (table).

        The result is a multiplication of the price table
        for the exchange table. If any of the tables is not available method
        return None.

        :return: Total table - multiplication of the price table
        for the exchange table or None.
        """
        if self._df_raw['prices'] is None:
            return None
        elif self._df_currency_raw is None:
            self._df_currency_raw = self.__get_currency_raw
            if self._df_currency_raw is None:
                return None
        return self._df_raw['prices'].mul(self._df_currency_raw)

    def _get_full_range_for_dates(self) -> \
            Tuple[Union[None, pd.DatetimeIndex], List]:
        """
        The method is needed to get full DatetimeIndex for all
        DateFrames with dates as index if index is not corrupted,
        If some index is corrupted, then it does not participate in
        the reindexing and the table becomes invalid

        All tables with dates merged into one and a full range
        is obtained for all dates.
        In fact, it corresponds to the range for the exchange table
        in a specific example.

        :return: Common DatetimeIndex for all DataFrames or None
        and list of keys with corrupted index
        """

        frames, bad_frames = [], []
        for key, data in self._df_raw.items():
            if type(data) != pd.DataFrame or data.index.name != 'dates':
                continue
            try:
                # check corrupted index
                pd.to_datetime(data.index)
                frames.append(data)
            except ValueError:
                bad_frames.append(key)

        if not frames:
            return None, bad_frames

        concat_frame = pd.concat(pd.Series(frames.index)
                                 for frames in frames)
        min_date, max_date = concat_frame.min(), concat_frame.max()
        date_index = pd.date_range(start=min_date, end=max_date)
        date_index.name = 'dates'
        return date_index, bad_frames

    @staticmethod
    def _normalize_a_frame(df: pd.DataFrame) -> None:
        """
        The method filled NaN in input DataFrame with ffillna method.

        It's propagate last valid observation forward to next valid.
        NaN at 1st row remain NaN!

        :param df: DateFrame for normalization on the basis of the last
        valid (elimination of NaN uncertainty).
        :return: Normalized DateFrame inplace.
        """

        df.fillna(method='ffill', inplace=True)

    def _generate_asset(self) -> None:
        """
        The method is a wrapper over self.__get_an_attitude.
        Coordination method for calculate R[i,t].
        :return: Assigns a new value to self._df_asset.
        """

        if self._df_asset is None:
            self._df_asset = self.__get_an_attitude(self._df_raw['prices'])

    def _generate_currency(self) -> None:
        """
        The method is a wrapper over self.__get_an_attitude.
        Coordination method for calculate CR[i,t].
        :return: Assigns a new value to self._df_currency.
        """
        if self._df_currency_raw is None:
            self._df_currency_raw = self.__get_currency_raw
        if self._df_currency is None:
            self._df_currency = self.__get_an_attitude(self._df_currency_raw)

    def _generate_total(self) -> None:
        """
        The method is a wrapper over self.__get_an_attitude.
        Coordination method for calculate TR[i,t].
        :return: Assigns a new value to self._df_total.
        """

        if self._df_total is None:
            self._df_total = self.__get_an_attitude(self.__get_total_raw)


class PortfolioData(FormalData):
    """PortfolioData is heir of FormalData and a parent of
    PortfolioPerformanceData. Functional class. No data is stored here
    """

    @df_checker
    def __get_a_portfolio(self, df: pd.DataFrame) -> Union[pd.DataFrame, None]:
        """
        The method is needed to get the portfolio.

        The method for calculating the portfolio by multiplying
        the previously calculated table by the weight
        and summing the columns

        :param df: R[i,t], CR[i,t] or TR[i,t].
        :return: Portfolio DataFrame
        """

        if self._df_raw['weights'] is None:
            return None
        return df.mul(self._df_raw['weights']).sum(axis=1, skipna=False)

    @property
    def _get_asset_portfolio(self) -> Union[pd.DataFrame, None]:
        """
        The method is a wrapper over self.__get_a_portfolio.
        Coordination method for calculate R[t].
        :return: Calculated R[t] or None.
        """

        self._generate_asset()
        return self.__get_a_portfolio(self._df_asset)

    @property
    def _get_currency_portfolio(self) -> Union[pd.DataFrame, None]:
        """
        The method is a wrapper over self.__get_a_portfolio.
        Coordination method for calculate CR[t].
        :return: Calculated CR[t] or None.
        """

        self._generate_currency()
        return self.__get_a_portfolio(self._df_currency)

    @property
    def _get_total_portfolio(self) -> Union[pd.DataFrame, None]:
        """
        The method is a wrapper over self.__get_a_portfolio.
        Coordination method for calculate TR[t].
        :return: Calculated TR[t] or None.
        """

        self._generate_total()
        return self.__get_a_portfolio(self._df_total)


class PortfolioPerformanceData(PortfolioData):
    """PortfolioPerformanceData is a heir of PortfolioData and
    FormalData. Functional class. No data is stored here.
    Initializes all data and sends data to FormData.
    """

    def __init__(self, path_with_data: str = os.path.join(
        os.getcwd(), 'Data'), silent: bool = False
                 ) -> None:
        """
        Load, check, normalize and send data from csv files
        to parent - FormalData.

        :param path_with_data: path to the folder with input files,
        it has a default value is ./Data
        """

        super().__init__()
        data_dict = {}
        keys_for_reindex = []
        for key, row_name in zip(
                ('currencies', 'exchanges', 'prices', 'weights'),
                ('asset id', *('dates',) * 3)):
            full_file_name = os.path.join(path_with_data, key + '.csv')
            data_dict.update(
                {key: pd.read_csv(
                    full_file_name, parse_dates=[0],
                    index_col=0) if os.path.exists(full_file_name) else None})
            if data_dict[key] is None:
                continue
            try:
                data_dict[key].index.name = row_name
                if row_name == 'dates':
                    keys_for_reindex.append(key)
                    data_dict[key] = data_dict[key].apply(
                        pd.to_numeric, errors='coerce')
            except AttributeError:
                pass
        self._df_raw = data_dict
        general_index, bad_keys = self._get_full_range_for_dates()
        # reindex current date frames or dropping them
        for key in keys_for_reindex:
            if key in bad_keys:
                if not silent:
                    print("{} index is corrupted and it"
                          " will not be used".format(key))
                self._df_raw[key] = None
                continue
            self._df_raw[key] = self._df_raw[key].reindex(
                general_index)
            self._normalize_a_frame(self._df_raw[key])

    @staticmethod
    @df_checker
    def __portfolio_performance(df, start_date: Union[
            pd.core.api.Timestamp, str, int], end_date: Union[
            pd.core.api.Timestamp, str, int], name: str) ->\
            Union[pd.Series, None]:
        """
        The method is needed to get the portfolio performance.

        Y{t}=Y{t-1}(1+X{t}) = DF.add(1).cumprod() if Y{0} = 1.
        In this case we can add 1 to all lines - add(1) and
        sequentially multiply all strings to previous ones - cumprod().

        :param df: R[t], CR[t] or TR[t].
        :param start_date: left border slice for date (including).
        :param end_date: right border slice for date (excluding).
        :param name: name of result pandas Series.
        :return: result of the calculation is taken
        on the cut of the date.
        """
        return df.add(1).cumprod().rename(name)[start_date:end_date]

    @date_checker
    def calculate_asset_performance(self, start_date: Union[
            pd.core.api.Timestamp, str, int], end_date: Union[
            pd.core.api.Timestamp, str, int]) -> Union[pd.Series, None]:

        """
        self.__portfolio_performance method wrapper
        for asset performance (R[t]).
        :param start_date: same as in self.__portfolio_performance.
        :param end_date: same as in self.__portfolio_performance.
        :return: result of self.__portfolio_performance -
        calculated pd.Series.
        """
        return self.__portfolio_performance(self._get_asset_portfolio,
                                            start_date, end_date, 'Pt')

    @date_checker
    def calculate_currency_performance(self, start_date: Union[
            pd.core.api.Timestamp, str, int], end_date: Union[
            pd.core.api.Timestamp, str, int]) -> Union[pd.Series, None]:
        """
        self.__portfolio_performance method wrapper
        for currency performance (CR[t]).
        :param start_date: same as in self.__portfolio_performance.
        :param end_date: same as in self.__portfolio_performance.
        :return: result of self.__portfolio_performance -
        calculated pd.Series.
        """

        return self.__portfolio_performance(self._get_currency_portfolio,
                                            start_date, end_date, 'CPt')

    @date_checker
    def calculate_total_performance(self, start_date: Union[
            pd.core.api.Timestamp, str, int], end_date: Union[
            pd.core.api.Timestamp, str, int]) -> Union[pd.Series, None]:
        """
        self.__portfolio_performance method wrapper
        for total performance (TR[t]).
        :param start_date: same as in self.__portfolio_performance.
        :param end_date: same as in self.__portfolio_performance.
        :return: result of self.__portfolio_performance -
        calculated pd.Series.
        """

        return self.__portfolio_performance(self._get_total_portfolio,
                                            start_date, end_date, 'TPt')
