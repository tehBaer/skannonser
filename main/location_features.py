"""
Flexible framework for calculating location-based features for addresses.

This module provides an extensible system for adding location-based features
to property/listing data. Features can be easily added, configured, and customized.

Example features:
- Walking distance to nearest grocery store
- Commuting time to a specific work address
- Walking time to nearest public transport stops
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
import requests
from geopy.geocoders import Nominatim
from geopy.distance import geodesic


def _next_monday_iso(hour: int, minute: int = 0) -> str:
    """Return ISO timestamp (UTC-formatted) for next Monday at given hour/minute."""
    now = datetime.now()
    days_ahead = 0 - now.weekday()  # Monday is 0
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    next_monday = now + timedelta(days=days_ahead)
    departure_time = next_monday.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return departure_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_departure_time(departure_time: Optional[Union[int, str, datetime]], default_hour: int) -> str:
    """Resolve departure time to an ISO timestamp string (UTC-formatted)."""
    if departure_time is None:
        return _next_monday_iso(default_hour)
    if isinstance(departure_time, int):
        return _next_monday_iso(departure_time)
    if isinstance(departure_time, datetime):
        return departure_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(departure_time)


class LocationFeature(ABC):
    """Base class for all location-based features."""

    def __init__(self, feature_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize a location feature.
        
        Args:
            feature_name: Name of the feature (e.g., 'walking_distance_to_grocery')
            config: Optional configuration dictionary for the feature
        """
        self.feature_name = feature_name
        self.config = config or {}
        self.geocoder = Nominatim(user_agent="location_features")

    @abstractmethod
    def calculate(self, address: str) -> Optional[Any]:
        """
        Calculate the feature value for a given address.
        
        Args:
            address: The address to calculate the feature for
            
        Returns:
            The calculated feature value or None if calculation fails
        """
        pass

    def get_coordinates(self, address: str) -> Optional[tuple]:
        """
        Get latitude and longitude for an address.
        
        Args:
            address: Address to geocode
            
        Returns:
            Tuple of (latitude, longitude) or None if geocoding fails
        """
        try:
            location = self.geocoder.geocode(address, timeout=10)
            if location:
                return (location.latitude, location.longitude)
        except Exception as e:
            print(f"Error geocoding address '{address}': {e}")
        return None


class WalkingDistanceToGrocery(LocationFeature):
    """Calculate walking distance to the nearest grocery store."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("walking_distance_to_grocery", config)
        self.api_key = config.get("api_key") if config else os.getenv("OSMNX_API_KEY")

    def calculate(self, address: str) -> Optional[int]:
        """
        Calculate walking distance to nearest grocery store.
        
        Returns:
            Integer distance in meters or None if calculation fails
        """
        coords = self.get_coordinates(address)
        if not coords:
            return None

        try:
            # Using OpenStreetMap Nominatim for nearby search
            lat, lon = coords
            
            # Overpass API query for grocery stores
            overpass_url = "https://overpass-api.de/api/interpreter"
            overpass_query = f"""
            [bbox:{lat-0.05},{lon-0.05},{lat+0.05},{lon+0.05}];
            (
                node["shop"="supermarket"];
                node["shop"="grocery"];
                way["shop"="supermarket"];
                way["shop"="grocery"];
            );
            out center 1;
            """
            
            response = requests.get(overpass_url, params={"data": overpass_query}, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("elements"):
                return None
            
            # Find nearest grocery store
            min_distance = float('inf')
            for element in data["elements"]:
                if "center" in element:
                    store_coords = (element["center"]["lat"], element["center"]["lon"])
                    distance = geodesic(coords, store_coords).kilometers
                    min_distance = min(min_distance, distance)
            
            if min_distance != float('inf'):
                # Approximate walking distance as 1.3x straight-line distance and convert to meters
                walking_distance_m = int(min_distance * 1.3 * 1000)
                return walking_distance_m
                
        except Exception as e:
            print(f"Error calculating grocery distance for '{address}': {e}")
        
        return None


class CommutingTimeToWorkAddress(LocationFeature):
    """Calculate commuting time to a specific work address using Google Maps API."""

    def __init__(self, work_address: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("commuting_time_to_work", config)
        self.work_address = work_address
        
        # Try to get API key from config, environment, or config file
        self.api_key = None
        if config and "api_key" in config:
            self.api_key = config["api_key"]
        elif os.getenv("GOOGLE_MAPS_API_KEY"):
            self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        else:
            # Try to import from config file
            try:
                from main.config.config import GOOGLE_MAPS_API_KEY
                self.api_key = GOOGLE_MAPS_API_KEY
            except ImportError:
                try:
                    from config.config import GOOGLE_MAPS_API_KEY
                    self.api_key = GOOGLE_MAPS_API_KEY
                except ImportError:
                    pass
        
        if not self.api_key or self.api_key == "your-google-maps-api-key-here":
            print("Warning: GOOGLE_MAPS_API_KEY not configured. Please set it in config.py or as an environment variable.")

    def calculate(
        self,
        address: str,
        postnummer: str = None,
        departure_time: Optional[Union[int, str, datetime]] = None,
        origin_override: Optional[str] = None,
        destination_override: Optional[str] = None,
    ) -> Optional[int]:
        """
        Calculate commuting time using Google Maps Routes API.
        Returns integer minutes only.
        
        Args:
            address: Origin address
            postnummer: Optional postal code (improves accuracy)
        
        Returns:
            Integer minutes or None if calculation fails
        """
        if not self.api_key:
            print("\n      ⚠️  No Google Maps API key configured")
            return None

        try:
            # Combine address with postal code if provided
            if origin_override:
                origin = origin_override
            else:
                origin = f"{address}, {postnummer}" if postnummer else address

            destination = destination_override or self.work_address
            
            # Using Google Maps Routes API
            base_url = "https://routes.googleapis.com/directions/v2:computeRoutes"
            
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.api_key,
                "X-Goog-FieldMask": "routes.duration,routes.distanceMeters"
            }
            
            # Default: next Monday at 8:00 AM
            departure_time_str = _resolve_departure_time(departure_time, default_hour=8)
            
            body = {
                "origin": {"address": origin},
                "destination": {"address": destination},
                "travelMode": "DRIVE",
                "routingPreference": "TRAFFIC_AWARE",
                "departureTime": departure_time_str
            }
            
            response = requests.post(base_url, json=body, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"\n      ⚠️  API error {response.status_code}: {response.text[:200]}")
                return None
            
            data = response.json()
            if "routes" in data and data["routes"]:
                route = data["routes"][0]
                if "duration" in route:
                    duration_str = route["duration"]
                    # Parse duration like "2700s" to seconds
                    duration_seconds = int(duration_str.rstrip('s'))
                    minutes = int(duration_seconds / 60)
                    return minutes
                return None
            else:
                print(f"\n      ⚠️  No routes found in response")
                return None
                
        except Exception as e:
            print(f"\n      ⚠️  Exception: {str(e)}")
            return None


class PublicTransitCommuteTime(LocationFeature):
    """Calculate total commuting time to work using public transport."""

    def __init__(self, work_address: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize public transit commute time calculator.
        
        Args:
            work_address: The work address to calculate commute time to
            config: Optional configuration
        """
        super().__init__("public_transit_commute_time", config)
        self.work_address = work_address
        
        # Try to get API key from config, environment, or config file
        self.api_key = None
        if config and "api_key" in config:
            self.api_key = config["api_key"]
        elif os.getenv("GOOGLE_MAPS_API_KEY"):
            self.api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        else:
            # Try to import from config file
            try:
                from main.config.config import GOOGLE_MAPS_API_KEY
                self.api_key = GOOGLE_MAPS_API_KEY
            except ImportError:
                try:
                    from config.config import GOOGLE_MAPS_API_KEY
                    self.api_key = GOOGLE_MAPS_API_KEY
                except ImportError:
                    pass
        
        if not self.api_key or self.api_key == "your-google-maps-api-key-here":
            print("Warning: GOOGLE_MAPS_API_KEY not configured. Please set it in config.py or as an environment variable.")

    def calculate(
        self,
        address: str,
        postnummer: str = None,
        departure_time: Optional[Union[int, str, datetime]] = None,
        origin_override: Optional[str] = None,
        destination_override: Optional[str] = None,
    ) -> Optional[int]:
        """
        Calculate total commuting time using public transit via Google Maps Routes API.
        Returns integer minutes only.
        
        Args:
            address: Origin address
            postnummer: Optional postal code (improves accuracy)
        
        Returns:
            Integer minutes or None if calculation fails
        """
        if not self.api_key:
            print("\n      ⚠️  No Google Maps API key configured")
            return None

        try:
            # Combine address with postal code if provided
            if origin_override:
                origin = origin_override
            else:
                origin = f"{address}, {postnummer}" if postnummer else address

            destination = destination_override or self.work_address
            
            # Using Google Maps Routes API with TRANSIT travel mode
            base_url = "https://routes.googleapis.com/directions/v2:computeRoutes"
            
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.api_key,
                "X-Goog-FieldMask": "routes.duration,routes.distanceMeters"
            }
            
            # Default: next Monday at 8:00 AM
            departure_time_str = _resolve_departure_time(departure_time, default_hour=8)
            
            body = {
                "origin": {"address": origin},
                "destination": {"address": destination},
                "travelMode": "TRANSIT",
                "departureTime": departure_time_str
            }
            
            response = requests.post(base_url, json=body, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"\n      ⚠️  API error {response.status_code}: {response.text[:200]}")
                return None
            
            data = response.json()
            if "routes" in data and data["routes"]:
                route = data["routes"][0]
                if "duration" in route:
                    duration_str = route["duration"]
                    # Parse duration like "2700s" to seconds
                    duration_seconds = int(duration_str.rstrip('s'))
                    minutes = int(duration_seconds / 60)
                    return minutes
                return None
            else:
                print(f"\n      ⚠️  No routes found in response")
                return None
                
        except Exception as e:
            print(f"\n      ⚠️  Exception: {str(e)}")
            return None


class WalkingTimeToPublicTransit(LocationFeature):
    """Calculate walking time to nearest tram, bus, or train stop."""

    def __init__(self, transit_type: str = "all", config: Optional[Dict[str, Any]] = None):
        """
        Initialize transit walking time calculator.
        
        Args:
            transit_type: "tram", "bus", "train", or "all" (default)
            config: Optional configuration
        """
        super().__init__(f"walking_time_to_{transit_type}", config)
        self.transit_type = transit_type

    def calculate(self, address: str) -> Optional[int]:
        """
        Calculate walking time to nearest public transit stop.
        
        Returns:
            Integer minutes or None if calculation fails
        """
        coords = self.get_coordinates(address)
        if not coords:
            return None

        try:
            lat, lon = coords
            
            # Build Overpass query based on transit type
            if self.transit_type == "all":
                query_filter = """
                (
                    node["public_transport"="stop_position"]["bus"="yes"];
                    node["public_transport"="stop_position"]["tram"="yes"];
                    node["public_transport"="stop_position"]["train"="yes"];
                    node["highway"="bus_stop"];
                    node["railway"="tram_stop"];
                    node["railway"="station"];
                );
                """
            elif self.transit_type == "bus":
                query_filter = """
                (
                    node["public_transport"="stop_position"]["bus"="yes"];
                    node["highway"="bus_stop"];
                );
                """
            elif self.transit_type == "tram":
                query_filter = """
                (
                    node["public_transport"="stop_position"]["tram"="yes"];
                    node["railway"="tram_stop"];
                );
                """
            elif self.transit_type == "train":
                query_filter = """
                (
                    node["railway"="station"];
                );
                """
            else:
                return None
            
            overpass_url = "https://overpass-api.de/api/interpreter"
            overpass_query = f"""
            [bbox:{lat-0.03},{lon-0.03},{lat+0.03},{lon+0.03}];
            {query_filter}
            out center 1;
            """
            
            response = requests.get(overpass_url, params={"data": overpass_query}, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if not data.get("elements"):
                return None
            
            # Find nearest transit stop
            min_distance = float('inf')
            for element in data["elements"]:
                if "center" in element:
                    stop_coords = (element["center"]["lat"], element["center"]["lon"])
                    distance = geodesic(coords, stop_coords).kilometers
                    min_distance = min(min_distance, distance)
            
            if min_distance != float('inf'):
                # Assume 1.4 km/hour walking speed = 84 m/min
                walking_minutes = int((min_distance * 1000) / 84)
                return walking_minutes
                
        except Exception as e:
            print(f"Error calculating transit time for '{address}': {e}")
        
        return None


class LocationFeaturesCalculator:
    """Main class to manage and calculate all location features."""

    def __init__(self):
        """Initialize the location features calculator."""
        self.features: Dict[str, LocationFeature] = {}

    def register_feature(self, feature: LocationFeature) -> None:
        """
        Register a location feature.
        
        Args:
            feature: An instance of LocationFeature subclass
        """
        self.features[feature.feature_name] = feature
        print(f"Registered feature: {feature.feature_name}")

    def register_builtin_features(self, work_address: str) -> None:
        """
        Register all built-in location features.
        
        Args:
            work_address: The work address for commuting calculations
        """
        self.register_feature(WalkingDistanceToGrocery())
        self.register_feature(CommutingTimeToWorkAddress(work_address))
        self.register_feature(WalkingTimeToPublicTransit("tram"))
        self.register_feature(WalkingTimeToPublicTransit("bus"))
        self.register_feature(WalkingTimeToPublicTransit("train"))

    def calculate_all(self, address: str) -> Dict[str, Optional[Any]]:
        """
        Calculate all registered features for an address.
        
        Args:
            address: The address to calculate features for
            
        Returns:
            Dictionary with feature_name -> value mappings
        """
        results = {}
        for feature_name, feature in self.features.items():
            print(f"Calculating {feature_name} for {address}...")
            try:
                value = feature.calculate(address)
                results[feature_name] = value
            except Exception as e:
                print(f"Error calculating {feature_name}: {e}")
                results[feature_name] = None
        return results

    def calculate_feature(self, address: str, feature_name: str) -> Optional[Any]:
        """
        Calculate a specific feature for an address.
        
        Args:
            address: The address to calculate the feature for
            feature_name: Name of the feature to calculate
            
        Returns:
            The calculated feature value or None if feature not found or fails
        """
        if feature_name not in self.features:
            print(f"Feature '{feature_name}' not registered")
            return None
        
        return self.features[feature_name].calculate(address)

    def get_feature_columns(self) -> List[str]:
        """
        Get list of all registered feature column names.
        
        Returns:
            List of feature names
        """
        return list(self.features.keys())


# Example usage
if __name__ == "__main__":
    # Initialize calculator
    calculator = LocationFeaturesCalculator()
    
    # Register built-in features with a work address
    work_address = "Oslo, Norway"  # Change this to your work address
    calculator.register_builtin_features(work_address)
    
    # You can also register custom features
    # calculator.register_feature(MyCustomFeature())
    
    # Calculate features for an address
    test_address = "Ferner Jacobsens gate 5, Oslo, Norway"
    results = calculator.calculate_all(test_address)
    
    print("\nCalculated features:")
    for feature_name, value in results.items():
        print(f"  {feature_name}: {value}")
