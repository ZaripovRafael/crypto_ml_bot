from binance.client import Client
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from os import environ
from dataclasses import dataclass

load_dotenv()


@dataclass
class MainCredentials:
    PUBLIC_KEY: str = environ.get('PUBLIC_KEY')
    SECRET_KEY: str = environ.get('SECRET_KEY')


class CompressorMixin:

    @staticmethod
    def _reduce_mem_usage(df: pd.DataFrame) -> pd.DataFrame:
        start_mem = df.memory_usage().sum() / 1024 ** 2
        for col in df.columns:
            col_type = df[col].dtypes
            if str(col_type)[:5] == "float":
                c_min = df[col].min()
                c_max = df[col].max()
                if c_min > np.finfo("f2").min and c_max < np.finfo("f2").max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo("f4").min and c_max < np.finfo("f4").max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
            elif str(col_type)[:3] == "int":
                c_min = df[col].min()
                c_max = df[col].max()
                if c_min > np.iinfo("i1").min and c_max < np.iinfo("i1").max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo("i2").min and c_max < np.iinfo("i2").max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo("i4").min and c_max < np.iinfo("i4").max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo("i8").min and c_max < np.iinfo("i8").max:
                    df[col] = df[col].astype(np.int64)
            elif col == "timestamp":
                df[col] = pd.to_datetime(df[col])
            elif str(col_type)[:8] != "datetime":
                df[col] = df[col].astype("category")
        end_mem = df.memory_usage().sum() / 1024 ** 2
        return df


class TradePlatformDataScrapper:

    @classmethod
    def __client_create(cls) -> Client:
        pass


    def get_historical_data(self):
        pass



class BinanceData(MainCredentials, TradePlatformDataScrapper, CompressorMixin):

    @classmethod
    def __client_create(cls) -> Client:
        """
        Create BinanceAPI client
        :return: Client
        """
        return Client(cls.PUBLIC_KEY, cls.SECRET_KEY)

    def get_historical_data(
            self,
            couple_coin: str = "BTCUSDT",
            date_start: str = "1 Jan, 2023",
            date_end: str = "14 Jan, 2023"
    ) -> pd.DataFrame:
        """
        Gets historical data for a five-minute interval within a specified time period
        :param couple_coin: str 'BTCUSDT'
        :param date_start: str "1 Oct, 2022"
        :param date_end: str "4 Jan, 2023"
        :return: Dataframe
        """
        client: Client = BinanceData.__client_create()
        k_lines: pd.DataFrame = pd.DataFrame(
            client.get_historical_klines(
                couple_coin,
                Client.KLINE_INTERVAL_5MINUTE,
                date_start,
                date_end
            )
        )

        k_lines = k_lines.iloc[:, :6]
        k_lines.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        k_lines = CompressorMixin._reduce_mem_usage(k_lines)
        k_lines = k_lines.set_index('Time')
        k_lines.index = pd.to_datetime(k_lines.index, unit='ms')

        return k_lines
