from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import urllib3
import requests
from urllib3 import exceptions

from config import settings

urllib3.disable_warnings(exceptions.InsecureRequestWarning)


def requests_retry_session(
        retries=1,
        backoff_factor=1,
        status_forcelist=(400, 403, 500, 502, 503, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    proxies = get_proxy(package='ads')
    if proxies:
        session.proxies = proxies
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.verify = False
    return session


def get_proxy(package: str, count: int = 1, country: str | None = None) -> dict | None:
    params = dict(package=package, count=count)
    if country:
        params['country'] = country
    if settings.PROXY_SERVICE:
        response = requests.get(url=settings.PROXY_SERVICE, timeout=5, params=params)
        if response.ok:
            proxy_info = response.json()
            user = proxy_info.get('username')
            password = proxy_info.get('password')
            address = proxy_info.get('ip')
            port = proxy_info.get('port_http')

            prefix = f'{user}:{password}@' if user else ''
            proxies = dict(http=f'http://{prefix}{address}:{port}', https=f'http://{prefix}{address}:{port}')
            return proxies
    else:
        return None
