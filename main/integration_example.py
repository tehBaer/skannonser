"""
Example integration of location features into the extraction pipeline.

This module demonstrates how to use the LocationFeaturesCalculator
to add location-based features to extracted property data.
"""

from main.location_features import LocationFeaturesCalculator
import os
from typing import Optional, Dict, Any


class EiendomLocationFeatures:
    """Specialized handler for integrating location features into eiendom extraction."""

    def __init__(self, work_address: Optional[str] = None, enable_features: bool = True):
        """
        Initialize location features for eiendom data.
        
        Args:
            work_address: Your work address for commuting calculations.
                         If None, commuting feature will not be calculated.
            enable_features: Whether to enable location feature calculations
        """
        self.enable_features = enable_features
        self.calculator = LocationFeaturesCalculator()
        
        if enable_features and work_address:
            self.calculator.register_builtin_features(work_address)
            print(f"Location features initialized with work address: {work_address}")
        elif enable_features:
            print("Warning: enable_features is True but no work_address provided.")

    def get_location_features(self, address: str) -> Dict[str, Any]:
        """
        Get all location features for a given address.
        
        Args:
            address: The property address
            
        Returns:
            Dictionary with feature values
        """
        if not self.enable_features:
            return {}
        
        if not address or address.strip() == "":
            print("Empty address provided")
            return {col: None for col in self.calculator.get_feature_columns()}
        
        return self.calculator.calculate_all(address)

    def get_feature_column_names(self) -> list:
        """Get list of all feature column names."""
        return self.calculator.get_feature_columns()

    def add_features_to_data(self, data: Dict[str, Any], address: str) -> Dict[str, Any]:
        """
        Add location features to existing property data.
        
        Args:
            data: The extracted property data dictionary
            address: The property address
            
        Returns:
            The data dictionary with location features added
        """
        if not self.enable_features:
            return data
        
        features = self.get_location_features(address)
        data.update(features)
        return data


# Example usage in extraction_eiendom.py:
"""
from main.integration_example import EiendomLocationFeatures

# Initialize once at module level
location_features = EiendomLocationFeatures(
    work_address="Your Work Address, City",  # Set this to your work address
    enable_features=True
)

def extract_eiendom_data(url, index, projectName, auto_save_new=True, force_save=False):
    # ... existing extraction code ...
    
    data = {
        'Finnkode': url.split('finnkode=')[1],
        'Tilgjengelighet': tilgjengelig,
        'Adresse': address,
        'Postnummer': area,
        'Pris': buy_price,
        'URL': url,
        'Primærrom': sizes.get('info-primary-area'),
        'Internt bruksareal (BRA-i)': sizes.get('info-usable-i-area'),
        'Bruksareal': sizes.get('info-usable-area'),
        'Eksternt bruksareal (BRA-e)': sizes.get('info-usable-e-area'),
        'Balkong/Terrasse (TBA)': sizes.get('info-open-area'),
        'Bruttoareal': sizes.get('info-gross-area'),
    }
    
    # Add location features
    data = location_features.add_features_to_data(data, address)
    
    print(f'Index {index}: {data}')
    return data


def extractEiendomDataFromAds(projectName: str, urls: DataFrame, outputFileName: str):
    # ... rest of existing code ...
"""
