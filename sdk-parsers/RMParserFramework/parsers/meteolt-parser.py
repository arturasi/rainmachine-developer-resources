from RMParserFramework.rmParser import RMParser  # Mandatory include for parser definition
from RMUtilsFramework.rmLogging import log  # Optional include for logging
from RMUtilsFramework.rmTimeUtils import rmTimestampFromDateAsString
from RMUtilsFramework.rmUtils import distanceBetweenGeographicCoordinatesAsKm

import json  # Your parser needed libraries
import time

class MeteoLTWeather(RMParser):
    parserName = "meteo.lt weather parser"  # Your parser name
    parserDescription = "Parser of weather forecast for Lithuania available from api.meteo.lt"  # A short description of your parser
    parserForecast = True  # True if parser provides future forecast data
    parserHistorical = False  # True if parser also provides historical data (only actual observed data)
    parserInterval = 2 * 3600  # Your parser running interval in seconds, data will only be mixed in hourly intervals
    parserDebug = False

    params = {"nearestPlaceCode": ""}  # we will find nearest station automatically if it is not set
    defaultParams = {"nearestPlaceCode": ""}

    def loadJSONFromURLWithRetries(self, url):

        raw_data = None
        for tryCnt in range(3):  # to avoid infinite loop retry max 3 times
            time.sleep(0.3)  # delay to avoid IP ban for too frequent requests to API; 0.2 is not enough
            url_opened = self.openURL(url)
            if url_opened is not None:
                raw_data = url_opened.read()
                break  # stop retrying
            else:
                delay_value = 30*(tryCnt+1)      # increase delay 30s every retry
                log.info("Delay " + str(delay_value) + "s to resume after too frequent API requests")
                time.sleep(delay_value)  # delay to resume after too frequent API requests

        if raw_data is None:
            log.error("Failed to open \"" + url + "\"")
            return None

        json_data = json.loads(raw_data)
        if json_data is None:
            log.error("Failed to parse \"" + url + "\"")
            return None

        return json_data

    def findNearestPlaceCode(self, currentLatitude, currentLongitude):

        # get list of places
        jsonPlaces = self.loadJSONFromURLWithRetries("https://api.meteo.lt/v1/places")
        if jsonPlaces is None:
            log.error("Failed to parse meteo.lt places")
            return

        # now get info about every place and measure the distance to it
        minimalDistance = 0
        nearestPlaceCode = ""
        for place in jsonPlaces:
            placeCode = place["code"]

            # get info about place coordinate
            jsonPlace = self.loadJSONFromURLWithRetries("https://api.meteo.lt/v1/places/" + placeCode)
            if jsonPlace is None:
                log.error("Failed to parse meteo.lt place \"" + placeCode + "\"")
                continue

            placeLatitude = jsonPlace["coordinates"]["latitude"]
            placeLongitude = jsonPlace["coordinates"]["longitude"]
            distance = distanceBetweenGeographicCoordinatesAsKm(currentLatitude, currentLongitude, placeLatitude,
                                                                placeLongitude)
            # log.debug("Place: %s, distance: %f", placeCode, distance)

            if nearestPlaceCode is "" or distance < minimalDistance:
                # ok this place is better, save it
                nearestPlaceCode = placeCode
                minimalDistance = distance
                log.debug("Updating nearest place to '%s' with distance to it %f km", nearestPlaceCode, minimalDistance)

        if minimalDistance > 10:
            log.error(
                "Nearest meteo.lt station is too far, please check your coordinates, it must be within Lithuania, Europe")
            return ""
        else:
            log.info("Found nearest place '%s' with distance to it %f km", nearestPlaceCode, minimalDistance)
            return nearestPlaceCode

    def perform(self):  # The function that will be executed must have this name

        # get nearest place code
        if self.params["nearestPlaceCode"] == "":
            # find it
            log.info("Nearest place is not set yet, trying to find one...")
            self.params["nearestPlaceCode"] = self.findNearestPlaceCode(self.settings.location.latitude,
                                                                        self.settings.location.longitude)

        # if nearest place is still unknown this means we failed to find it; exit
        if self.params["nearestPlaceCode"] == "":
            log.error(
                "Failed to get nearest place. Please make sure that your coordinates are within Lithuania, Europe.")
            return

        # downloading data from a URL convenience function since other python libraries can be used
        jsonData =  self.loadJSONFromURLWithRetries("https://api.meteo.lt/v1/places/" + self.params["nearestPlaceCode"] + "/forecasts/long-term")
        if jsonData is None:
            log.error("Failed to get meteo.lt forecast JSON contents")
            return

        conditionAdapter = {
            "clear": RMParser.conditionType.Fair,
            "partly-cloudy": RMParser.conditionType.PartlyCloudy,
            "cloudy-with-sunny-intervals": RMParser.conditionType.FewClouds,
            "cloudy": RMParser.conditionType.Overcast,
            "thunder": RMParser.conditionType.ThunderstormInVicinity,
            "isolated-thunderstorms": RMParser.conditionType.ThunderstormInVicinity,
            "thunderstorms": RMParser.conditionType.Thunderstorm,
            "heavy-rain-with-thunderstorms": RMParser.conditionType.Thunderstorm,
            "light-rain": RMParser.conditionType.LightRain,
            "rain": RMParser.conditionType.LightRain,
            "heavy-rain": RMParser.conditionType.HeavyRain,
            "light-sleet": RMParser.conditionType.RainShowers,
            "sleet": RMParser.conditionType.RainSnow,
            "freezing-rain": RMParser.conditionType.FreezingRain,
            "hail": RMParser.conditionType.IcePellets,
            "light-snow": RMParser.conditionType.Snow,
            "snow": RMParser.conditionType.Snow,
            "heavy-snow": RMParser.conditionType.Snow,
            "fog": RMParser.conditionType.Fog,
            "null": RMParser.conditionType.Unknown
        }

        for forecast in jsonData["forecastTimestamps"]:
            # log.debug(forecast)
            timestamp = rmTimestampFromDateAsString(forecast["forecastTimeUtc"], "%Y-%m-%d %H:%M:%S")
            self.addValue(RMParser.dataType.TEMPERATURE, timestamp, forecast["airTemperature"])
            self.addValue(RMParser.dataType.WIND, timestamp, forecast["windSpeed"])
            self.addValue(RMParser.dataType.SKYCOVER, timestamp, forecast["cloudCover"] / 100)
            self.addValue(RMParser.dataType.PRESSURE, timestamp, forecast["seaLevelPressure"] * 0.1)
            self.addValue(RMParser.dataType.RH, timestamp, forecast["relativeHumidity"])
            self.addValue(RMParser.dataType.QPF, timestamp, forecast["totalPrecipitation"])
            self.addValue(RMParser.dataType.CONDITION, timestamp, conditionAdapter[forecast["conditionCode"]])


if __name__ == "__main__":
    p = MeteoLTWeather()
    p.perform()
