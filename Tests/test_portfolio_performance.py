# encoding: utf-8
import unittest
import os
import fileinput
import pandas as pd
from portfolio_performance import PortfolioPerformanceData


class TestDate(unittest.TestCase):
    def setUp(self):
        self.portfolio = PortfolioPerformanceData('../Data')
        self.main_functions = (self.portfolio.calculate_asset_performance,
                               self.portfolio.calculate_currency_performance,
                               self.portfolio.calculate_total_performance
                               )

    def _get_normal_date(self, args):
        """All options in this method are suitable for
        setting the date."""

        func1, func2, func3 = args
        self.assertIsNotNone(func1(20130201, "20190120"))
        self.assertIsNotNone(func2("2013/02/01", "2019-01-20"))
        self.assertIsNotNone(func3(r"2013-/\-02~@-\/-@~01",
                                   pd.to_datetime('2019-01-20')))

    def test_asset_normal_date(self):
        self._get_normal_date(
            (self.main_functions[0],)*3)

    def test_currency_normal_date(self):
        self._get_normal_date(
            (self.main_functions[1],)*3)

    def test_total_normal_date(self):
        self._get_normal_date(
            (self.main_functions[2],)*3)

    def test_all_in_normal_date(self):
        self._get_normal_date(self.main_functions)

    def test_bad_date(self):
        """If the date parameter task is incorrect,
        class methods shouldn't run at all."""

        for func in self.main_functions:
            with self.assertRaises(TypeError):
                func(20130201, 201901200)
                func('20130201', '201901200')
                func(2013020, 20190120)
                func('2013020', '20190120')

    def test_empty_dates(self):
        """If start_date>end_date results must be empty"""

        for func in self.main_functions:
            self.assertFalse(func(20190120, 20130201).size)


class TestInputData(unittest.TestCase):
    def setUp(self):
        self.data_path = '../Data'
        self.boarder = (20130201, 20190120)

    def test_bad_dirrectory_path(self):
        """The results of all methods should be None in case
        of selecting the wrong folder where there are no
        necessary files at all"""

        portfolio = PortfolioPerformanceData(self.data_path+'NonExisting')
        for func in (portfolio.calculate_asset_performance,
                     portfolio.calculate_currency_performance,
                     portfolio.calculate_total_performance):
            self.assertIsNone(func(*self.boarder))

    def _test_None_data(self, portfolio, bool_status):
        """This is auxiliary method checking None
        as a result of the methods"""
        data = (
            portfolio.calculate_asset_performance(*self.boarder),
            portfolio.calculate_currency_performance(*self.boarder),
            portfolio.calculate_total_performance(*self.boarder)
        )
        for frame, bool_value in zip(data, bool_status):
            self.assertIsNone(frame) if bool_value \
                else self.assertIsNotNone(frame)

    def test_bad_name_prices(self):
        """No found the prices.csv:
        calculate_asset_performance -> None
        calculate_currency_performance -> res
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'prices.csv')
        file_new = os.path.join(self.data_path, 'it_is_not_prices.csv')
        os.rename(file, file_new)
        portfolio = PortfolioPerformanceData(self.data_path)
        try:
            self._test_None_data(portfolio, (True, False, True))
        finally:
            os.rename(file_new, file)

    def test_bad_name_currencies(self):
        """No found the currencies.csv:
        calculate_asset_performance -> res
        calculate_currency_performance -> None
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'currencies.csv')
        file_new = os.path.join(self.data_path, 'it_is_not_currencies.csv')
        os.rename(file, file_new)
        portfolio = PortfolioPerformanceData(self.data_path)
        try:
            self._test_None_data(portfolio, (False, True, True))
        finally:
            os.rename(file_new, file)

    def test_bad_name_exchanges(self):
        """No found the exchanges.csv:
        calculate_asset_performance -> res
        calculate_currency_performance -> None
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'exchanges.csv')
        file_new = os.path.join(self.data_path, 'it_is_not_exchanges.csv')
        os.rename(file, file_new)
        portfolio = PortfolioPerformanceData(self.data_path)
        try:
            self._test_None_data(portfolio, (False, True, True))
        finally:
            os.rename(file_new, file)

    def test_bad_name_weights(self):
        """No found the weights.csv:
        calculate_asset_performance -> None
        calculate_currency_performance -> None
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'weights.csv')
        file_new = os.path.join(self.data_path, 'it_is_not_weights.csv')
        os.rename(file, file_new)
        portfolio = PortfolioPerformanceData(self.data_path)
        try:
            self._test_None_data(portfolio, (True, True, True))
        finally:
            os.rename(file_new, file)

    @staticmethod
    def _change_file(file):
        """This is auxiliary method to corrupt data in a file."""

        with fileinput.FileInput(file, inplace=True, backup='.bak') as f:
            for index, line in enumerate(f):
                if index == 13:
                    print(line.replace(line, line[15:]), end='')
                else:
                    print(line.replace(line, line), end='')

    @staticmethod
    def _restore_file(file):
        """This is auxiliary method to restore an original file."""

        os.remove(file)
        os.rename(file + '.bak', file)

    def test_corrupted_index_prices(self):
        """Corrupted index in the prices.csv:
        calculate_asset_performance -> None
        calculate_currency_performance -> res
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'prices.csv')
        try:
            self._change_file(file)
            portfolio = PortfolioPerformanceData(self.data_path, silent=True)
            self._test_None_data(portfolio, (True, False, True))
        finally:
            self._restore_file(file)

    def test_corrupted_index_exchanges(self):
        """Corrupted index in the exchanges.csv:
        calculate_asset_performance -> res
        calculate_currency_performance -> None
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'exchanges.csv')
        try:
            self._change_file(file)
            portfolio = PortfolioPerformanceData(self.data_path, silent=True)
            self._test_None_data(portfolio, (False, True, True))
        finally:
            self._restore_file(file)

    def test_corrupted_index_weights(self):
        """Corrupted index in the weights.csv:
        calculate_asset_performance -> None
        calculate_currency_performance -> None
        calculate_total_performance -> None
        """

        file = os.path.join(self.data_path, 'weights.csv')
        try:
            self._change_file(file)
            portfolio = PortfolioPerformanceData(self.data_path, silent=True)
            self._test_None_data(portfolio, (True, True, True))
        finally:
            self._restore_file(file)


class TestAlgorithms(unittest.TestCase):
    def setUp(self):
        self.portfolio = PortfolioPerformanceData('../Data')
        self.prices = self.portfolio._FormalData__df_raw_dict['prices']
        self.weights = self.portfolio._FormalData__df_raw_dict['weights']
        self.weights = self.weights[self.prices.columns]
        self.currency = self.portfolio._FormalData__get_currency_raw
        self.currency = self.currency[self.prices.columns]
        self.test_column_number = 0
        self.left_boarder = 0
        self.right_boarder = 10
        self.test_row_number = 10
        self.boarder = (20130201, 20190120)

    def _manual_calculate_formal(self, df_column):
        """This is auxiliary method:
        Y[i,t] = (X[i,t] - X[i,t-1]) / X[i,t-1]"""

        if type(df_column) != list:
            raw_column = self._convert_df_to_list(df_column)
        else:
            raw_column = df_column
        for index, value in enumerate(raw_column):
            if not self.left_boarder and index == self.left_boarder:
                yield float('NaN')
                continue
            yield(value-raw_column[index-1])/raw_column[index-1]

    def _convert_df_to_list(self, df):
        """This is auxiliary method:
        get column from df and convert it to list"""

        return df.iloc[
                      self.left_boarder:self.right_boarder,
                      self.test_column_number].to_list()

    @staticmethod
    def _clear_column(column):
        """This is auxiliary method: drop NaNs from list"""

        return [i for i in column if str(i) != 'nan']

    def test_Rit(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: R[i,t]"""

        manual_calculated = list(self._manual_calculate_formal(self.prices))

        self.portfolio._generate_asset()
        test_column = self._convert_df_to_list(self.portfolio._df_asset)

        self.assertAlmostEqual(self._clear_column(manual_calculated),
                               self._clear_column(test_column))

    def test_CRit(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: CR[i,t]"""

        manual_calculated = list(self._manual_calculate_formal(self.currency))

        self.portfolio._generate_currency()
        test_column = self._convert_df_to_list(self.portfolio._df_currency)
        self.assertAlmostEqual(self._clear_column(manual_calculated),
                               self._clear_column(test_column))

    @staticmethod
    def list_multiplication(list1, list2):
        """This is auxiliary method: LISTxLIST by elements"""
        return [p*c for p, c in zip(list1, list2)]

    def test_TRit(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: TR[i,t]"""

        prices = self._convert_df_to_list(self.prices)
        currency = self._convert_df_to_list(self.currency)

        manual_total = self.list_multiplication(prices, currency)
        manual_calculated = list(self._manual_calculate_formal(manual_total))

        self.portfolio._generate_total()
        test_column = self._convert_df_to_list(self.portfolio._df_total)

        self.assertAlmostEqual(self._clear_column(manual_calculated),
                               self._clear_column(test_column))

    def test_Rt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: R[t]"""

        test_value = self.portfolio._get_asset_portfolio[self.test_row_number]

        test_prices = self.portfolio._df_asset.iloc(axis=0)[
            self.test_row_number].values
        test_weights = self.weights.iloc(axis=0)[
            self.test_row_number].values
        calculated_value = sum(self.list_multiplication(test_prices,
                                                        test_weights))
        self.assertAlmostEqual(test_value, calculated_value)

    def test_CRt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: CR[t]"""

        test_value = self.portfolio._get_currency_portfolio[
            self.test_row_number]

        test_currency = self.portfolio._df_currency[
            self.prices.columns].iloc(axis=0)[self.test_row_number].values
        test_weights = self.weights.iloc(axis=0)[
            self.test_row_number].values
        calculated_value = sum(self.list_multiplication(test_currency,
                                                        test_weights))
        self.assertAlmostEqual(test_value, calculated_value)

    def test_TRt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: TR[t]"""

        test_value = self.portfolio._get_total_portfolio[
            self.test_row_number]

        test_total = self.portfolio._df_total.iloc(axis=0)[
            self.test_row_number].values
        test_weights = self.weights.iloc(axis=0)[
            self.test_row_number].values
        calculated_value = sum(self.list_multiplication(test_total,
                                                        test_weights))
        self.assertAlmostEqual(test_value, calculated_value)

    def manual_cumprod(self, series):
        """This is auxiliary method: Y[t] = Y[t-1]*(X[t]+1)"""
        values = self._clear_column(series[0: self.test_row_number+1].values)
        results = [1]
        for index, value in enumerate(values):
            results.append(results[index]*(value+1))
        return results[-1]

    def test_Pt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: P[t]"""

        test_value = self.portfolio.calculate_asset_performance(
            *self.boarder)[self.test_row_number]
        calculated_value = self.manual_cumprod(
            self.portfolio._get_asset_portfolio)
        self.assertAlmostEqual(test_value, calculated_value)

    def test_CPt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: CP[t]"""

        test_value = self.portfolio.calculate_currency_performance(
            *self.boarder)[self.test_row_number]
        calculated_value = self.manual_cumprod(
            self.portfolio._get_currency_portfolio)
        self.assertAlmostEqual(test_value, calculated_value)

    def test_TPt(self):
        """Compares the value obtained in the class and calculated
        here with primitive types: TP[t]"""

        test_value = self.portfolio.calculate_total_performance(
            *self.boarder)[self.test_row_number]
        calculated_value = self.manual_cumprod(
            self.portfolio._get_total_portfolio)
        self.assertAlmostEqual(test_value, calculated_value)
