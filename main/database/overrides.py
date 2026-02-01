"""
Manual overrides manager for property data.
Handles override operations for correcting property data (areal, pris).
"""
import sqlite3
from typing import Optional, Tuple, List
import os


class PropertyOverrides:
    """Manages manual overrides for property data."""
    
    def __init__(self, db_path: str = None):
        """Initialize with database path."""
        if db_path is None:
            # Default to database folder
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, 'properties.db')
        
        self.db_path = db_path
    
    def _get_connection(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path)
    
    def set_override(self, finnkode: str, areal: int = None, pris: int = None, reason: str = None):
        """
        Set manual override for a property's areal and/or pris.
        
        Args:
            finnkode: The property's finn.no code
            areal: Override value for area (square meters)
            pris: Override value for price
            reason: Explanation for the override
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Check if override already exists
        cursor.execute('SELECT id FROM manual_overrides WHERE finnkode = ?', (finnkode,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing override
            cursor.execute('''
                UPDATE manual_overrides
                SET areal = COALESCE(?, areal),
                    pris = COALESCE(?, pris),
                    override_reason = COALESCE(?, override_reason),
                    updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
            ''', (areal, pris, reason, finnkode))
        else:
            # Insert new override
            cursor.execute('''
                INSERT INTO manual_overrides (finnkode, areal, pris, override_reason)
                VALUES (?, ?, ?, ?)
            ''', (finnkode, areal, pris, reason))
        
        conn.commit()
        conn.close()
        
        print(f"✓ Override set for {finnkode}: areal={areal}, pris={pris}, reason={reason}")
    
    def get_override(self, finnkode: str) -> Optional[Tuple[Optional[int], Optional[int], Optional[str]]]:
        """
        Get override values for a property if they exist.
        
        Args:
            finnkode: The property's finn.no code
            
        Returns:
            Tuple of (areal, pris, reason) or None if no override exists
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT areal, pris, override_reason FROM manual_overrides WHERE finnkode = ?', (finnkode,))
        result = cursor.fetchone()
        conn.close()
        
        return result
    
    def list_overrides(self) -> List[Tuple]:
        """
        List all active overrides.
        
        Returns:
            List of tuples: (finnkode, areal, pris, reason, updated_at)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT finnkode, areal, pris, override_reason, updated_at 
            FROM manual_overrides 
            ORDER BY updated_at DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            print("No overrides set")
            return []
        
        print("\n📋 Manual Overrides:")
        for finnkode, areal, pris, reason, updated_at in results:
            print(f"  {finnkode}:")
            if areal is not None:
                print(f"    - AREAL: {areal}")
            if pris is not None:
                print(f"    - PRIS: {pris}")
            if reason:
                print(f"    - Reason: {reason}")
            print(f"    - Updated: {updated_at}")
        
        return results
    
    def remove_override(self, finnkode: str) -> int:
        """
        Remove manual override for a property.
        
        Args:
            finnkode: The property's finn.no code
            
        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM manual_overrides WHERE finnkode = ?', (finnkode,))
        deleted = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if deleted > 0:
            print(f"✓ Override removed for {finnkode}")
        else:
            print(f"✗ No override found for {finnkode}")
        
        return deleted
    
    def apply_overrides_to_data(self, finnkode: str, data: dict) -> dict:
        """
        Apply overrides to a data dictionary if they exist.
        
        Args:
            finnkode: The property's finn.no code
            data: Dictionary containing property data
            
        Returns:
            Updated data dictionary with overrides applied
        """
        override = self.get_override(finnkode)
        if override:
            if override[0] is not None:  # areal override exists
                data['areal'] = override[0]
            if override[1] is not None:  # pris override exists
                data['pris'] = override[1]
        
        return data
