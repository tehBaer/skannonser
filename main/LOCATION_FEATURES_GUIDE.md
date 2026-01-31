# Location Features Guide

## Overview

The location features system provides a flexible, extensible framework for adding location-based data to property/listing information. It's designed to be easily customizable and allows you to add new features without modifying the core code.

## Features Included

### Built-in Features

1. **Walking Distance to Nearest Grocery Store** (`walking_distance_to_grocery`)
   - Finds the nearest supermarket or grocery store
   - Returns walking distance in kilometers
   - Uses OpenStreetMap data

2. **Commuting Time to Work Address (Driving)** (`commuting_time_to_work`)
   - Calculates driving time from property to your work address
   - Returns time in minutes
   - Uses Google Maps Routes API

3. **Public Transit Commute Time** (`public_transit_commute_time`) - Maps to **PENDLEVEI** column
   - Calculates TOTAL commuting time using public transit from property to work address
   - Returns time in minutes
   - Includes walking to transit stop + transit travel time
   - Uses Google Maps Routes API with TRANSIT mode

4. **Walking Time to Nearest Public Transit Stop** (`walking_time_to_all`)
   - Finds nearest tram, bus, or train stop
   - Returns walking time in minutes
   - Maps to **GÅ_TID_TIL_STOPP** column (future)

5. **Walking Time to Tram Stop** (`walking_time_to_tram`)
   - Finds nearest tram/streetcar stop
   - Returns walking time in minutes

6. **Walking Time to Bus Stop** (`walking_time_to_bus`)
   - Finds nearest bus stop
   - Returns walking time in minutes

7. **Walking Time to Train Station** (`walking_time_to_train`)
   - Finds nearest train/railway station
   - Returns walking time in minutes

## Quick Start

### Basic Usage

```python
from main.location_features import LocationFeaturesCalculator

# Initialize the calculator
calculator = LocationFeaturesCalculator()

# Register built-in features with your work address
work_address = "123 Work Street, Oslo, Norway"
calculator.register_builtin_features(work_address)

# Calculate features for an address
address = "Ferner Jacobsens gate 5, Oslo, Norway"
results = calculator.calculate_all(address)

print(results)
# Output:
# {
#     'walking_distance_to_grocery': 850,  # meters
#     'commuting_time_to_work': 25,  # driving time in minutes
#     'public_transit_commute_time': 35,  # PENDLEVEI - total public transit time in minutes
#     'walking_time_to_all': 8,  # walking to nearest stop in minutes
#     'walking_time_to_tram': 5,
#     'walking_time_to_bus': 3,
#     'walking_time_to_train': 12
# }
```

### Integration with Extraction Pipeline

Use the `EiendomLocationFeatures` helper class for seamless integration:

```python
from main.integration_example import EiendomLocationFeatures

# Initialize at module level
location_features = EiendomLocationFeatures(
    work_address="Your Work Address, City",
    enable_features=True
)

# In your extraction function
def extract_eiendom_data(url, index, projectName, ...):
    # ... existing code ...
    
    data = {
        'Finnkode': ...,
        'Adresse': address,
        # ... other fields ...
    }
    
    # Add location features automatically
    data = location_features.add_features_to_data(data, address)
    
    return data
```

## Adding Custom Features

You can easily add custom location features by creating a subclass of `LocationFeature`:

```python
from main.location_features import LocationFeature

class WalkingDistanceToSchool(LocationFeature):
    """Calculate walking distance to nearest school."""
    
    def __init__(self, config=None):
        super().__init__("walking_distance_to_school", config)
    
    def calculate(self, address: str):
        coords = self.get_coordinates(address)
        if not coords:
            return None
        
        # Your implementation here using Overpass API or similar
        # Return the calculated value
        
        return "0.80 km"  # Example output

# Register your custom feature
calculator = LocationFeaturesCalculator()
calculator.register_feature(WalkingDistanceToSchool())
```

### Custom Feature Template

```python
from main.location_features import LocationFeature
from typing import Optional, Dict, Any

class CustomLocationFeature(LocationFeature):
    """Brief description of what this feature calculates."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("feature_name", config)
        # Initialize any needed resources
    
    def calculate(self, address: str) -> Optional[Any]:
        """
        Calculate the feature for a given address.
        
        Args:
            address: The address to calculate for
            
        Returns:
            The calculated value or None if calculation fails
        """
        # Get coordinates of the address
        coords = self.get_coordinates(address)
        if not coords:
            return None
        
        try:
            # Your calculation logic here
            # You can use self.get_coordinates() to geocode addresses
            # and external APIs for routing/distance calculations
            
            result = "your_calculated_value"
            return result
        except Exception as e:
            print(f"Error in calculation: {e}")
            return None
```

## Available Methods in LocationFeature

### `get_coordinates(address: str) -> Optional[tuple]`
Converts an address to latitude/longitude coordinates.
- Uses OpenStreetMap's Nominatim geocoder
- Returns: (latitude, longitude) tuple or None

### `calculate(address: str) -> Optional[Any]`
Abstract method that must be implemented in subclasses.
- Should return the calculated feature value or None

## Configuration

### Work Address Configuration

```python
# Set this to your actual work address
work_address = "Your Company Name, Street Address, City, Country"

# Then use it:
calculator.register_builtin_features(work_address)
```

### Custom Feature Configuration

```python
config = {
    "api_key": "your_api_key_here",
    "timeout": 15,
    "custom_param": "value"
}

feature = MyCustomFeature(config=config)
calculator.register_feature(feature)
```

## Data Sources

- **Geocoding**: OpenStreetMap Nominatim
- **POI Search**: Overpass API (OpenStreetMap)
- **Routing**: OSRM (Open Source Routing Machine)
- **Distance Calculation**: GeoPy library

All data sources are free and open-source!

## Performance Considerations

- API calls are made individually per address per feature
- Consider caching results if processing large datasets
- Walking/driving times are approximations based on typical speeds
- Some API calls may take 2-10 seconds

## Handling Errors

The system gracefully handles failures:
- Invalid addresses return None
- Failed API calls return None
- Features continue calculating even if one fails

```python
results = calculator.calculate_all(address)

# Check for successful calculations
for feature_name, value in results.items():
    if value is None:
        print(f"Could not calculate {feature_name}")
    else:
        print(f"{feature_name}: {value}")
```

## Extending the System

### Example: Adding a "Distance to Shopping Center" Feature

```python
from main.location_features import LocationFeature, LocationFeaturesCalculator
import requests
from geopy.distance import geodesic

class DistanceToShoppingCenter(LocationFeature):
    def __init__(self, config=None):
        super().__init__("distance_to_shopping_center", config)
        self.shopping_centers = {
            "Storo Sentrum": (59.9192, 10.7692),
            "Oslo City": (59.9118, 10.7537),
            # Add more shopping centers...
        }
    
    def calculate(self, address: str):
        coords = self.get_coordinates(address)
        if not coords:
            return None
        
        min_distance = float('inf')
        nearest_center = None
        
        for center_name, center_coords in self.shopping_centers.items():
            distance = geodesic(coords, center_coords).kilometers
            if distance < min_distance:
                min_distance = distance
                nearest_center = center_name
        
        if min_distance != float('inf'):
            return f"{nearest_center}: {min_distance:.2f} km"
        
        return None

# Use it
calculator = LocationFeaturesCalculator()
calculator.register_feature(DistanceToShoppingCenter())
```

## Troubleshooting

### API Rate Limiting
If you get rate limit errors, add delays between requests:
```python
import time
for address in addresses:
    results = calculator.calculate_all(address)
    time.sleep(1)  # Wait 1 second between requests
```

### Geocoding Failures
If addresses aren't being geocoded:
- Make sure the address format is correct
- Include city and country for better accuracy
- Try slightly different address formats

### Missing Values
Some features may return None if:
- The address is invalid or not found
- There are no relevant POIs nearby
- Network/API issues occur

## Dependencies

Required packages (see requirements.txt):
- geopy>=2.4.1
- requests (already in requirements)
- Other standard libraries

Install with:
```bash
pip install -r requirements.txt
```

## Examples for Different Use Cases

### Real Estate (Eiendom)
```python
location_features = EiendomLocationFeatures(
    work_address="Oslo Business District, Oslo",
    enable_features=True
)
```

### Rental Properties (Leie)
```python
from main.location_features import LocationFeaturesCalculator

calculator = LocationFeaturesCalculator()
calculator.register_builtin_features("Oslo, Norway")
# Add it to your rental extraction pipeline
```

### Job Listings
```python
# For job postings, you might care more about public transit
calculator = LocationFeaturesCalculator()
calculator.register_feature(WalkingTimeToPublicTransit("all"))
calculator.register_feature(WalkingDistanceToGrocery())
```

## Future Enhancement Ideas

- Add caching to avoid recalculating for the same address
- Support for multiple work addresses (car pool coordination)
- Price-per-kilometer calculations
- School proximity and ratings
- Park/green space accessibility
- Air quality/pollution data
- Noise level measurements
- Neighborhood crime statistics
