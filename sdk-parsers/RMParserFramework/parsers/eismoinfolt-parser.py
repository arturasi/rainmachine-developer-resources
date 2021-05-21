from RMParserFramework.rmParser import RMParser  # Mandatory include for parser definition
from RMUtilsFramework.rmLogging import log  # Optional include for logging
from RMUtilsFramework.rmUtils import distanceBetweenGeographicCoordinatesAsKm

import json  # Your parser needed libraries


class EismoinfoLTWeather(RMParser):
    parserName = "eismoinfo.lt retrospective weather parser"  # Your parser name
    parserDescription = "Parser of retrospective weather data from weather stations along Lithuania roads"  # A short description of your parser
    parserForecast = False  # True if parser provides future forecast data
    parserHistorical = True  # True if parser also provides historical data (only actual observed data)
    parserInterval = 1 * 3600  # Your parser running interval in seconds, data will only be mixed in hourly intervals
    parserDebug = False

    params = {
        "nearestStationID": "",  # we will find nearest station automatically if it is not set
        "SkipStationIDs": "1164"    # list of stations ID to skip (comma separated); some stations provide wrong rain values
        }
    defaultParams = {"nearestStationID": "", "SkipStationIDs": "1164"}

    def findNearestStationID(self, currentLatitude, currentLongitude):

        # get list of places
        rawStations = self.openURL("http://eismoinfo.lt/weather-conditions-service").read()
        if rawStations is None:
            log.error("Failed to get eismoinfo.lt stations")
            return

        stationIDsToSkip=self.params["SkipStationIDs"].split(",")

        jsonStations = json.loads(rawStations)
        if jsonStations is None:
            log.error("Failed to parse eismoinfo.lt stations")
            return

        # now get info about every place and measure the distance to it
        minimalDistance = 0
        nearestStationID = ""
        for station in jsonStations:
            # log.debug(station)

            # check that station should have rain sensor
            if station["krituliu_kiekis"] is None:
                continue

            stationID = station["id"]
            if stationID in stationIDsToSkip:
                log.debug("Skipping stationID %s", stationID)
                continue

            # get info about place coordinate
            stationLatitude = float(station["lat"])
            stationLongitude = float(station["lng"])
            distance = distanceBetweenGeographicCoordinatesAsKm(currentLatitude, currentLongitude,
                                                                stationLatitude, stationLongitude)
            # log.debug("StationID: %s, distance: %f", stationID, distance)

            if nearestStationID == "" or distance < minimalDistance:
                # ok this place is better, save it
                nearestStationID = stationID
                minimalDistance = distance
                log.debug("Updating nearest station ID to %s with distance to it %f km", nearestStationID,
                          minimalDistance)

        if minimalDistance > 20:
            log.error(
                "Nearest eismoinfo.lt station is too far, please check your coordinates, they must be within Lithuania, Europe")
            return ""
        else:
            log.info("Found nearest station ID %s with distance to it %f km", nearestStationID, minimalDistance)
            return nearestStationID

    def perform(self):  # The function that will be executed must have this name

        # check do we already know nearest place code
        if self.params["nearestStationID"] == "":
            log.info("eismoinfo.lt nearest place is not set yet, trying to find one...")
            # self.params["nearestStationID"] = self.findNearestStationID(54.64727905936214,25.109561061574105)
            self.params["nearestStationID"] = self.findNearestStationID(self.settings.location.latitude,
                                                                        self.settings.location.longitude)

            if self.params["nearestStationID"] == "":
                log.error("Failed to find nearest station, please recheck your current coordinates")
                return

        # downloading data from a URL convenience function since other python libraries can be used
        rawData = self.openURL(
            "http://eismoinfo.lt/weather-conditions-retrospective?id=" + self.params["nearestStationID"]).read() + \
            "&number=500"
        if rawData is None:
            log.error("Failed to get eismoinfo.lt contents")
            return

        jsonData = json.loads(rawData)
        if jsonData is None:
            log.error("Failed to get eismoinfo.lt JSON contents")
            return

        for history in jsonData:
            log.debug(history)
            timestamp = int(history["surinkimo_data_unix"])
            self.addValue(RMParser.dataType.TEMPERATURE, timestamp, float(history["oro_temperatura"]))
            self.addValue(RMParser.dataType.WIND, timestamp, float(history["vejo_greitis_vidut"]))
            self.addValue(RMParser.dataType.RAIN, timestamp, float(history["krituliu_kiekis"]))


if __name__ == "__main__":
    p = EismoinfoLTWeather()
    p.perform()
