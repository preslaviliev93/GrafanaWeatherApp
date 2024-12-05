import configparser

import requests
from prometheus_client import start_http_server, Gauge
import time
from configparser import ConfigParser
import logging


class WeatherConfig:
    def __init__(self, config_file: str) -> None:
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.weather_api = self.config['WeatherAPI']
        self.prometheus = self.config['Prometheus']
        self.logging = self.config['Logging']

    def get_weather_api_config(self):
        return {
            "api_key": self.weather_api['api_key'],
            "base_url": self.weather_api['base_url'],
            "city": self.weather_api['city'],
            "country": self.weather_api['country'],
        }

    def get_prometheus_config(self):
        return {
            "port": self.prometheus['port'],
            "scrape_interval": self.prometheus['scrape_interval'],
        }

    def get_logging_config(self):
        return {
            "log_file": self.logging['log_file'],
            "log_level": self.logging['log_level'],
        }

class WeatherDataFetcher:
    def __init__(self, api_key, base_url, city, country):
        self.api_key = api_key
        self.base_url = base_url
        self.city = city
        self.country = country

    def fetch_weather_data(self):
        params = {
            "q": f"{self.city},{self.country}",
            "appid": self.api_key,
            "units": "metric",
        }
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            return {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data["wind"]["speed"],
                "wind_gust": data["wind"]["gust"],
                "wind_bearing": data["wind"]["bearing"],
                "wind_dir": data["wind"]["dir"],
            }
        except Exception as e:
            logging.error(f"Failed to fetch weather data: {e}")
            return None


class PrometheusMetrics:

    def __init__(self, city):
        self.temperature_gauge = Gauge('weather_temperature', 'Temperature in Celsius', ['city'])
        self.humidity_gauge = Gauge('weather_humidity', 'Humidity in percentage', ['city'])
        self.pressure_gauge = Gauge('weather_pressure', 'Pressure in hPa', ['city'])
        self.city = city

    def update_metrics(self, data):
        self.temperature_gauge.labels(city=self.city).set(data['temperature'])
        self.humidity_gauge.labels(city=self.city).set(data['humidity'])
        self.pressure_gauge.labels(city=self.city).set(data['pressure'])


class WeatherMonitor:
    """Coordinates weather data fetching and Prometheus updates."""
    def __init__(self, config_file):
        self.config = WeatherConfig(config_file)
        api_config = self.config.get_weather_api_config()
        prometheus_config = self.config.get_prometheus_config()
        logging_config = self.config.get_logging_config()

        # Initialize components
        self.data_fetcher = WeatherDataFetcher(
            api_key=api_config['api_key'],
            base_url=api_config['base_url'],
            city=api_config['city'],
            country=api_config['country']
        )
        self.prometheus_metrics = PrometheusMetrics(city=api_config['city'])
        self.prometheus_port = prometheus_config['port']
        self.scrape_interval = prometheus_config['scrape_interval']

        # Set up logging
        logging.basicConfig(
            filename=logging_config['log_file'],
            level=logging_config['log_level'],
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def start(self):
        """Starts the weather monitor."""
        start_http_server(int(self.prometheus_port))
        logging.info(f"Prometheus HTTP server started on port {self.prometheus_port}")

        while True:
            weather_data = self.data_fetcher.fetch_weather_data()
            if weather_data:
                logging.info(f"Weather data fetched: {weather_data}")
                self.prometheus_metrics.update_metrics(weather_data)
            else:
                logging.warning("Failed to fetch weather data.")
            time.sleep(int(self.scrape_interval))


if __name__ == "__main__":
    # Main entry point
    monitor = WeatherMonitor('config.ini')
    monitor.start()
