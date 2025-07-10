from typing import Union

from pydantic_settings import BaseSettings
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Settings(BaseSettings):

    SOURCE_IP: Union[str, None] = None

    APPLICATION_NAME: str

    DB_PORT: str
    DB_HOST: str
    DB_PASS: str
    DB_USER: str
    DB_NAME: str
    DB_SCHEMA: str

    PROXY_SERVICE: str


settings = Settings()
