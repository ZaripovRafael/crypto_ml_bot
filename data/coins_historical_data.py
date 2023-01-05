from binance.client import Client
import pandas as pd
import time
from dotenv import load_dotenv
from os import environ
from dataclasses import dataclass

load_dotenv()


@dataclass
class MainCredentials:
    PUBLIC_KEY: str = environ.get('PUBLIC_KEY')
    SECRET_KEY: str = environ.get('SECRET_KEY')


class BinanceData(MainCredentials):

    @classmethod
    def __client_create(cls) -> Client:
        """
        Create BinanceAPI client
        :return: Client
        """
        return Client(cls.PUBLIC_KEY, cls.SECRET_KEY)

    @staticmethod
    def __get_historical_data(couple_coin: str, date_start: str, date_end: str) -> pd.DataFrame:
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
                "BTCUSDT",
                Client.KLINE_INTERVAL_5MINUTE,
                "1 Oct, 2022",
                "4 Jan, 2023"
            )
        )

        k_lines = k_lines.iloc[:, :6]
        k_lines.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        k_lines = k_lines.set_index('Time')
        k_lines.index = pd.to_datetime(k_lines.index, unit='ms')
        k_lines = k_lines.astype('float')
        return k_lines

    @staticmethod
    def __data_compressor(df: pd.DataFrame) -> pd.DataFrame:
        """
        Compresses the size of a dataframe by changing its data types
        :param df: Pandas Dataframe
        :return: compressed Pandas Dataframe
        """
        TODO: 'Нужно прописать алгоритм сжимания данных по столбцам датафрейма'
        pass
